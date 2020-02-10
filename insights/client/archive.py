"""
Handle adding files and preparing the archive for upload
"""
from __future__ import absolute_import
import os
import shutil
import subprocess
import shlex
import logging
import tempfile
import atexit

from .utilities import write_data_to_file

logger = logging.getLogger(__name__)


class InsightsArchive(object):
    """
    This class is an interface for adding command output
    and files to the insights archive
    """
    def __init__(self, config):
        """
        Initialize the Insights Archive
        Create temp dir, archive dir, and command dir
        """
        self.config = config
        self.tmp_dir = tempfile.mkdtemp(prefix='/var/tmp/')
        self.archive_tmp_dir = None
        if not self.config.obfuscate:
            self.archive_tmp_dir = tempfile.mkdtemp(prefix='/var/tmp/')
        name = determine_hostname()
        # archive_name, archive_dir to be filled in once insights.collect.collect() is run
        self.archive_name = None
        self.archive_dir = None
        self.compressor = config.compressor
        self.tar_file = None
        atexit.register(self.cleanup_tmp)

    def update(self, collected_data_path):
        self.archive_dir = collected_data_path
        self.archive_name = os.path.basename(collected_data_path) or 'insights-archive'

    def get_full_archive_path(self, path):
        """
        Returns the full archive path
        """
        return os.path.join(self.archive_dir, path.lstrip('/'))

    def get_compression_flag(self, compressor):
        return {
            "gz": "z",
            "xz": "J",
            "bz2": "j",
            "none": ""
        }.get(compressor, "z")

    def create_tar_file(self):
        """
        Create tar file to be compressed
        """
        if not self.archive_tmp_dir:
            # we should never get here but bail out if we do
            raise RuntimeError('Archive temporary directory not defined.')
        tar_file_name = os.path.join(self.archive_tmp_dir, self.archive_name)
        ext = "" if self.compressor == "none" else ".%s" % self.compressor
        tar_file_name = tar_file_name + ".tar" + ext
        logger.debug("Tar File: " + tar_file_name)
        return_code = subprocess.call(shlex.split("tar c%sfS %s -C %s ." % (
            self.get_compression_flag(self.compressor),
            tar_file_name, self.tmp_dir)),
            stderr=subprocess.PIPE)
        if (self.compressor in ["bz2", "xz"] and return_code != 0):
            logger.error("ERROR: %s compressor is not installed, cannot compress file", self.compressor)
            return None
        self.delete_archive_dir()
        logger.debug("Tar File Size: %s", str(os.path.getsize(tar_file_name)))
        self.tar_file = tar_file_name
        return tar_file_name

    def delete_tmp_dir(self):
        """
        Delete the entire tmp dir
        """
        logger.debug("Deleting: " + self.tmp_dir)
        shutil.rmtree(self.tmp_dir, True)

    def delete_archive_dir(self):
        """
        Delete the entire archive dir
        """
        logger.debug("Deleting: " + self.archive_dir)
        shutil.rmtree(self.archive_dir, True)

    def delete_archive_file(self):
        """
        Delete the directory containing the constructed archive
        """
        if self.archive_tmp_dir:
            logger.debug("Deleting %s", self.archive_tmp_dir)
            shutil.rmtree(self.archive_tmp_dir, True)

    def add_metadata_to_archive(self, metadata, meta_path):
        '''
        Add metadata to archive
        '''
        archive_path = self.get_full_archive_path(meta_path.lstrip('/'))
        write_data_to_file(metadata, archive_path)

    def cleanup_tmp(self):
        '''
        Only used during built-in collection.
        Delete archive and tmp dirs on exit unless --keep-archive is specified
            and tar_file exists.
        '''
        if self.config.keep_archive and self.tar_file:
            if self.config.no_upload:
                logger.info('Archive saved at %s', self.tar_file)
            else:
                logger.info('Insights archive retained in %s', self.tar_file)
            if self.config.obfuscate:
                return  # return before deleting tmp_dir
        else:
            self.delete_archive_file()
        self.delete_tmp_dir()
