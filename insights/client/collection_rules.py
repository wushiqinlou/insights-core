"""
Categorize the commands, paths, and template strings used by datasources,
and parse the remove.conf file.
"""
from __future__ import absolute_import
import logging
import six
import os
import yaml
import stat
from six.moves import configparser as ConfigParser
from .constants import InsightsConstants as constants
from collections import defaultdict
from insights import datasource, dr, parse_plugins, load_packages
from insights.core import spec_factory as sf

APP_NAME = constants.app_name
logger = logging.getLogger(__name__)
NETWORK = constants.custom_network_log_level

expected_keys = ('commands', 'files', 'patterns', 'keywords')


def resolve(d):
    """
    Categorizes a datasource's command, path, or template information.

    The categorization ignores first_of, head, and find since they depend on other
    datasources that will get resolved anyway. Ignore the listdir helper and explicit
    @datasource functions since they're pure python.
    """
    if isinstance(d, sf.simple_file):
        return ("file_static", [d.path])

    if isinstance(d, sf.first_file):
        return ("file_static", d.paths)

    if isinstance(d, sf.glob_file):
        return ("file_glob", d.patterns)

    if isinstance(d, sf.foreach_collect):
        return ("file_template", [d.path])

    if isinstance(d, sf.simple_command):
        return ("command_static", [d.cmd])

    if isinstance(d, sf.command_with_args):
        return ("command_template", [d.cmd])

    if isinstance(d, sf.foreach_execute):
        return ("command_template", [d.cmd])

    return (None, None)


def categorize(ds):
    """
    Extracts commands, paths, and templates from datasources and cateorizes them
    based on their type.
    """
    results = defaultdict(set)
    for d in ds:
        (cat, res) = resolve(d)
        if cat is not None:
            results[cat] |= set(res)
    return {k: sorted(v) for k, v in results.items()}


def get_spec_report():
    """
    You'll need to already have the specs loaded, and then you can call this
    procedure to get a categorized dict of the commands we might run and files
    we might collect.
    """
    load("insights.specs.default")
    ds = dr.get_components_of_type(datasource)
    return categorize(ds)


# helpers for running the script directly
# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("-p", "--plugins", default=)
#     return p.parse_args()


def load(p):
    plugins = parse_plugins(p)
    load_packages(plugins)


# def main():
#     args = parse_args()
#     load(args.plugins)
#     report = get_spec_report()
#     print(yaml.dump(report))


class InsightsUploadConf(object):
    """
    Insights spec configuration from uploader.json
    """

    def __init__(self, config):
        """
        Load config from parent
        """
        self.remove_file = config.remove_file

    def get_rm_conf_old(self):
        """
        Get excluded files config from remove_file.
        """
        # Convert config object into dict
        logger.debug('Trying to parse as INI file.')
        parsedconfig = ConfigParser.RawConfigParser()

        try:
            parsedconfig.read(self.remove_file)
            rm_conf = {}
            for item, value in parsedconfig.items('remove'):
                if item not in expected_keys:
                    raise RuntimeError('Unknown section in remove.conf: ' + item +
                                       '\nValid sections are ' + ', '.join(expected_keys) + '.')
                if six.PY3:
                    rm_conf[item] = value.strip().encode('utf-8').decode('unicode-escape').split(',')
                else:
                    rm_conf[item] = value.strip().decode('string-escape').split(',')

            return rm_conf
        except ConfigParser.Error as e:
            # can't parse config file at all
            logger.debug(e)
            raise RuntimeError('ERROR: Cannot parse the remove.conf file as a YAML file '
                               'nor as an INI file. Please check the file formatting.\n'
                               'See %s for more information.' % self.config.logging_file)

    def get_rm_conf(self):
        '''
        Load remove conf. If it's a YAML-formatted file, try to load
        the "new" version of remove.conf
        '''
        def is_list_of_strings(data):
            '''
            Helper function for correct_format()
            '''
            if data is None:
                # nonetype, no data to parse. treat as empty list
                return True
            if not isinstance(data, list):
                return False
            for l in data:
                if not isinstance(l, six.string_types):
                    return False
            return True

        def correct_format(parsed_data):
            '''
            Ensure the parsed file matches the needed format
            Returns True, <message> on error
            '''
            # validate keys are what we expect
            keys = parsed_data.keys()
            invalid_keys = set(keys).difference(expected_keys)
            if invalid_keys:
                return True, ('Unknown section(s) in remove.conf: ' + ', '.join(invalid_keys) +
                              '\nValid sections are ' + ', '.join(expected_keys) + '.')

            # validate format (lists of strings)
            for k in expected_keys:
                if k in parsed_data:
                    if k == 'patterns' and isinstance(parsed_data['patterns'], dict):
                        if 'regex' not in parsed_data['patterns']:
                            return True, 'Patterns section contains an object but the "regex" key was not specified.'
                        if 'regex' in parsed_data['patterns'] and len(parsed_data['patterns']) > 1:
                            return True, 'Unknown keys in the patterns section. Only "regex" is valid.'
                        if not is_list_of_strings(parsed_data['patterns']['regex']):
                            return True, 'regex section under patterns must be a list of strings.'
                        continue
                    if not is_list_of_strings(parsed_data[k]):
                        return True, '%s section must be a list of strings.' % k
            return False, None

        if not os.path.isfile(self.remove_file):
            logger.debug('No remove.conf defined. No files/commands will be ignored.')
            return None
        try:
            with open(self.remove_file) as f:
                rm_conf = yaml.safe_load(f)
            if rm_conf is None:
                logger.warn('WARNING: Remove file %s is empty.', self.remove_file)
                return {}
        except (yaml.YAMLError, yaml.parser.ParserError) as e:
            # can't parse yaml from conf, try old style
            logger.debug('ERROR: Cannot parse remove.conf as a YAML file.\n'
                         'If using any YAML tokens such as [] in an expression, '
                         'be sure to wrap the expression in quotation marks.\n\nError details:\n%s\n', e)
            return self.get_rm_conf_old()
        if not isinstance(rm_conf, dict):
            # loaded data should be a dict with at least one key (commands, files, patterns, keywords)
            logger.debug('ERROR: Invalid YAML loaded.')
            return self.get_rm_conf_old()
        err, msg = correct_format(rm_conf)
        if err:
            # YAML is correct but doesn't match the format we need
            raise RuntimeError('ERROR: ' + msg)
        # remove Nones, empty strings, and empty lists
        filtered_rm_conf = dict((k, v) for k, v in rm_conf.items() if v)
        return filtered_rm_conf

    def validate(self):
        '''
        Validate remove.conf
        '''
        if not os.path.isfile(self.remove_file):
            logger.warn("WARNING: Remove file does not exist")
            return False
        # Make sure permissions are 600
        mode = stat.S_IMODE(os.stat(self.remove_file).st_mode)
        if not mode == 0o600:
            logger.error("WARNING: Invalid remove file permissions. "
                         "Expected 0600 got %s" % oct(mode))
            return False
        else:
            logger.debug("Correct file permissions")
        success = self.get_rm_conf()
        if success is None or success is False:
            logger.error('Could not parse remove.conf')
            return False
        # Using print here as this could contain sensitive information
        if self.config.verbose or self.config.validate:
            print('Remove file parsed contents:')
            print(success)
            logger.info('Parsed successfully.')
        return True


if __name__ == '__main__':
    from .config import InsightsConfig
    print(InsightsUploadConf(InsightsConfig().load_all()))
