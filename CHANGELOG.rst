0.3.8
==================

- 增加几个 technical analysis 的 examples 和自动化测试
- 修复一些在 Python 2 下运行的 bug

0.3.7
==================

- 增加 :code:`-mc` / :code:`--mod-config` 参数来传递参数到 mod 中
- 增加了 simple_stock_realtime_trade, progressive_output_csv，funcat_api 几个 DEMO mod 供开发者参考开发自己的 mod
- :code:`update_bundle` 移到 :code:`main.py` 中，方便直接从代码中调用 :code:`update_bundle`
- 增加了一些自动化的测试用例

0.3.6
==================

- support auto test with Travis [python 2.7 3.4 3.5 3.6]
- :code:`rqalpha.run` 现在支持直接传入 :code:`source_code` 了
- 支持 :code:`rqalpha.update_bundle` 函数

0.3.5
==================

- 增加 :code:`from rqalpha import run` 接口，现在可以很方便的直接在程序中调用RQAlpha 回测了。

0.3.4
==================

- 本地化模块更具有扩展性
- 修改 :code:`rqalpha update_bundle` 的目录结构，现在是在指定目录下生成一个 bundle 文件，而不再会直接删除当前文件夹内容了。

0.3.3
==================

- 解决 :code:`rqalpha examples -d .` 无样例策略生成的问题

0.3.2
==================

- 解决 Windows 10 下 matplotlib 中文字体显示乱码的问题
- 解决 Windows 下 set_locale error 的问题

0.3.1
==================

- 增加 Python 2 的支持

0.3.0
==================

- 支持多周期回测扩展(虽然只有日线数据，但是结构上是支持不同周期的回测和实盘的)
- 支持期货策略
- 支持混合策略(股票和期货混合)
- 支持多种参数配置方式
- 抽离接口层，数据源、事件源、撮合引擎、下单模块全部可以替换或扩展。
- 完善事件定义，采取 pub/sub 模式，可以非常简答的在 RQAlpha 中添加 hook。
- 增加 Mod 机制，极大的增加了 RQAlpha 的扩展性，使其可以轻松完成程序化交易过程中所产生的的特定需求。

0.0.53
==================

- 完善了回测结果显示
- 修正了 Risk 计算和 Benchmark 计算


0.0.20
==================

- 增加会回测进度显示开关
- 完善了回测结果显示

0.0.19
==================

- 在 :code:`handle_bar` 前用当前的数据更新 portfolio 和 position，因为 ricequant.com 是这样做的。

0.0.18
==================

- 修复了分红计算

0.0.16
==================

- 用户可以通过 context 设置 slippage/commission/benchmark
- 增加了 scheduler

0.0.15
==================

- 修复 history 在 before_trading 调用
- 增加 api 的 phase 检查

0.0.14
==================

- 修改支持 python2

0.0.12
==================

- 修正了 Risk 计算，使用合理的年化收益计算方法
- 格式化代码符合 pep8
- 更新 requirements.txt


0.0.9
==================

- 增加了数据下载
- 修正了 Risk 计算
- 增加了 instrument
- 增加了 position 的 :code:`market_value` 和 :code:`value_percent`


0.0.2
==================

- 增加了日线回测
- 去掉了涨跌停检查
- 增加了分红处理
- 运行参数如下:

.. code-block:: python3

  # 生成sample策略
  rqalpha generate_examples -d ./

  # 运行回测
  rqalpha run -f examples/simple_macd.py -s 2013-01-01 -e 2015-01-04 -o /tmp/a.pkl

0.0.1
==================

- 搭建基本的框架，增加基本的 unittest
