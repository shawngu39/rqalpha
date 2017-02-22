# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click
import errno
import locale
import os
import shutil

from .cache_control import set_cache_policy, CachePolicy
from .utils.click_helper import Date
from .utils.i18n import localization
from .utils.config import parse_config


@click.group()
@click.option('-v', '--verbose', count=True)
@click.pass_context
def cli(ctx, verbose):
    ctx.obj["VERBOSE"] = verbose


def entry_point():
    cli(obj={})


@cli.command()
@click.option('-d', '--data-bundle-path', default=os.path.expanduser("~/.rqalpha"), type=click.Path(file_okay=False))
def update_bundle(data_bundle_path):
    from . import main
    main.update_bundle(data_bundle_path)


@cli.command()
@click.option('-d', '--data-bundle-path', 'base__data_bundle_path', type=click.Path(exists=True))
@click.option('-f', '--strategy-file', 'base__strategy_file', type=click.Path(exists=True))
@click.option('-s', '--start-date', 'base__start_date', type=Date())
@click.option('-e', '--end-date', 'base__end_date', type=Date())
@click.option('-r', '--rid', 'base__run_id', type=click.STRING)
@click.option('-i', '--init-cash', 'base__stock_starting_cash', type=click.FLOAT)
@click.option('-sc', '--stock-starting-cash', 'base__stock_starting_cash', type=click.FLOAT)
@click.option('-fc', '--future-starting-cash', 'base__future_starting_cash', type=click.FLOAT)
@click.option('-bm', '--benchmark', 'base__benchmark', type=click.STRING, default=None)
@click.option('-sp', '--slippage', 'base__slippage', type=click.FLOAT)
@click.option('-cm', '--commission-multiplier', 'base__commission_multiplier', type=click.FLOAT)
@click.option('-mm', '--margin-multiplier', 'base__margin_multiplier', type=click.FLOAT)
@click.option('-k', '--kind', 'base__strategy_type', type=click.Choice(['stock', 'future', 'stock_future']))
@click.option('-fq', '--frequency', 'base__frequency', type=click.Choice(['1d', '1m']))
@click.option('-me', '--match-engine', 'base__matching_type', type=click.Choice(['current_bar', 'next_bar']))
@click.option('-rt', '--run-type', 'base__run_type', type=click.Choice(['b', 'p']), default="b")
@click.option('--resume', 'base__resume_mode', is_flag=True)
@click.option('--handle-split/--not-handle-split', 'base__handle_split', default=None, help="handle split")
@click.option('--risk-grid/--no-risk-grid', 'base__cal_risk_grid', default=True)
@click.option('-l', '--log-level', 'extra__log_level', type=click.Choice(['verbose', 'debug', 'info', 'error']))
@click.option('-p', '--plot/--no-plot', 'extra__plot', default=None, help="plot result")
@click.option('-o', '--output-file', 'extra__output_file', type=click.Path(writable=True), help="output result pickle file")
@click.option('--fast-match', 'validator__fast_match', is_flag=True)
@click.option('--progress/--no-progress', 'mod__progress__enabled', default=None, help="show progress bar")
@click.option('--extra-vars', 'extra__context_vars', type=click.STRING, help="override context vars")
@click.option("--enable-profiler", "extra__enable_profiler", is_flag=True, help="add line profiler to profile your strategy")
@click.option('--config', 'config_path', type=click.STRING, help="config file path")
@click.option('-mc', '--mod-config', 'mod_configs', nargs=2, multiple=True, type=click.STRING, help="mod extra config")
@click.help_option('-h', '--help')
def run(**kwargs):
    if kwargs.get('base__run_type') == 'p':
        set_cache_policy(CachePolicy.MINIMUM)

    try:
        locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
        locale.setlocale(locale.LC_CTYPE, "en_US.UTF-8")
        os.environ['TZ'] = 'Asia/Shanghai'
        localization.set_locale(["zh_Hans_CN"])
    except Exception as e:
        if os.name != 'nt':
            raise

    config_path = kwargs.get('config_path', None)
    if config_path is not None:
        config_path = os.path.abspath(config_path)

    from . import main
    main.run(parse_config(kwargs, config_path))


@cli.command()
@click.option('-d', '--directory', default="./", type=click.Path(), required=True)
def examples(directory):
    """
    generate example strategies to target folder
    """
    source_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "examples")

    try:
        shutil.copytree(source_dir, os.path.join(directory, "examples"))
    except OSError as e:
        if e.errno == errno.EEXIST:
            print("Folder examples is exists.")


@cli.command()
@click.argument('result_dict_file', type=click.Path(exists=True), required=True)
@click.option('--show/--hide', 'is_show', default=True)
@click.option('--plot-save', 'plot_save_file', default=None, type=click.Path(), help="save plot result to file")
def plot(result_dict_file, is_show, plot_save_file):
    """
    draw result DataFrame
    """
    import pandas as pd
    from rqalpha.plot import plot_result

    result_dict = pd.read_pickle(result_dict_file)
    if is_show:
        plot_result(result_dict)
    if plot_save_file:
        plot_result(result_dict, show_windows=False, savefile=plot_save_file)


@cli.command()
@click.argument('result_pickle_file_path', type=click.Path(exists=True), required=True)
@click.argument('target_report_csv_path', type=click.Path(exists=True, writable=True), required=True)
def report(result_pickle_file_path, target_report_csv_path):
    """
    generate report from backtest output file
    """
    import pandas as pd
    result_dict = pd.read_pickle(result_pickle_file_path)

    from rqalpha import main

    main.generate_report(result_dict, target_report_csv_path)


@cli.command()
@click.option('-v', '--verbose', is_flag=True)
def version(**kwargs):
    """
    Output Version Info
    """
    from rqalpha import version_info
    print("Current Version: ", version_info)


if __name__ == '__main__':
    entry_point()
