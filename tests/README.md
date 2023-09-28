本项目使用 pybbt 工具进行黑盒测试。pybbt 是一个基于 Python 的工具，简化了软件的黑盒测试过程。要运行测试用例，请执行以下命令：

```bash
pybbt cases --verbose --flags modules
```
该命令将以详细日志记录的方式运行 cases 目录中的测试用例，并向测试用例传递 modules 标志。

如果本地没有安装 modules，可以不起用 modules 标志，这样就会跳过需要 modules 的测试用例:
```bash
pybbt cases --verbose
```

更多关于 pybbt 的信息、安装说明和用法示例，请参阅完整的文档: https://pypi.org/project/pybbt/