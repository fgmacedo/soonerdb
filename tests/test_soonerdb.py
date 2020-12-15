#!/usr/bin/env python

"""Tests for `soonerdb` package."""

import pytest

from click.testing import CliRunner

from soonerdb import cli


def test_empty_soonerdb(db):
    assert db.is_empty()


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'soonerdb.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output
