#!/usr/bin/python
# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import os
import sys
import argparse


from yoink import *


_DIRECT_COPY_PLUGIN_DEFAULT = 'ngamsDirectCopyDppi'
_PROLOGUE = \
    """This is my prologue"""
_EPILOGUE = \
    """This is my epilogue"""



def get_parser():
    """ Build and return an argument parser with the command line options for yoink; this is
        out here and not in a class because Sphinx needs it to build the docs. """
    parser = argparse.ArgumentParser(description=_PROLOGUE, epilog=_EPILOGUE,
                                     formatter_class=argparse.RawTextHelpFormatter)
    # Can't find a way of clearing the action groups without hitting an internal attribute.
    parser._action_groups.pop()
    required_group = parser.add_argument_group('Required Arguments')
    optional_group = parser.add_argument_group('Optional Arguments')
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()


if __name__ == '__main__':
    main()
