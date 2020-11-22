import os
import sys
import time
import argparse
import subprocess

from scrape import RemotePage


def read_csv(fn, sep=',', skip=0, col=None):
    try:
        count = 0
        with open(fn, 'r') as f:
            for i in f:
                count = count + 1
                if skip >= count:
                    continue
                cols = i.strip().split(sep)
                yield cols
    except Exception as e:
        print(e)
        return []


def start_master():
    print("master 进程启动:")

    # 从目标文件中读取结果
    res = {}
    for i in read_csv(args.result):
        if len(i) == 3 and i[2] != '':
            res[i[1]] = i[2]

    # 从源文件中读取 url
    titles = {}
    for i in read_csv(args.source, skip=1):
        titles[i[1]] = i[0]

    _urls = []
    for x in titles.keys():
        if not res.get(x) or args.refetch:
            _urls.append(x)

    # 去除已抓取并分组启动 worker
    # _urls = _urls[0:20]
    step = len(_urls) // args.num
    step = step if step > 0 else len(_urls)

    _tmpfs = []
    for i in range(0, len(_urls), step):
        fname = os.path.join(os.path.dirname(args.result),
                             '{0}-{1}.tmp'.format(i, i+step))
        _tmpfs.append(fname)
        _exec_slave(fname, _urls[i:i+step])
        # 拼接参数，启动任务
        # print(_urls[i:i+step])
    # 拿到 worker 的结果
    # 等待全部执行结束
    print("等待进程完成，数量:", len(_tmpfs))
    while len(list(filter(lambda i: i, map(lambda i: os.path.exists(i), _tmpfs)))) != len(_tmpfs):
        time.sleep(1)
    print("全部任务完成，合并数据")
    # 合并数据
    for fn in _tmpfs:
        for i in read_csv(fn):
            if len(i) < 2: continue
            res[i[0]] = i[1]
        # os.remove(fn)
    # res 写入文件
    with open(args.result, 'w') as f:
        for (x, y) in res.items():
            f.write('{0}, {1}, {2}\n'.format(titles[x.strip()], x, y))
    print("下载成功!")


def _exec_slave(name, urls):
    if os.path.exists(name): return
    cmd = 'python {0} --slave --name {1} --urls {2} &'.format(
        sys.argv[0], name, ' '.join(urls))
    # print(cmd)
    os.system(cmd)
    # pcmd = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    # (out, _) = pcmd.communicate()
    # print(cmd)
    # print(out)
    # res = out.split("\n")
    # for i in range(len(urls)):
    #     yield (urls[i], res[i])


def start_slave():
    # 从参数中给的URL启动抓取任务
    res = {}
    for u in args.urls:
        page = RemotePage(u)
        res[u] = ' | '.join(page.images)


    if args.name == "stdout":
        for (x, y) in res.items():
            print("{0}, {1}\n".format(x, y))
        return

    with open(args.name, "w") as f:
        for (x, y) in res.items():
            f.write("{0}, {1}\n".format(x, y))


def main():

    parser = argparse.ArgumentParser(description='上市公司图片链接抓取.')

    parser.add_argument("--source", default="./data/urls.csv", help="抓取链接的文件")
    parser.add_argument(
        "--result", default="./data/result.csv", help="抓取的结果输出文件")
    parser.add_argument("--num", default=100, help="worker 的数量")
    parser.add_argument("--refetch", action="store_true",
                        default=False, help="已经抓取过是否重新抓取")

    parser.add_argument("--slave", action='store_true',
                        default=False, help='是否子抓取进程')
    parser.add_argument("--name", default="stdout", help="结果输出文件名")
    parser.add_argument("--urls", metavar="URL", type=str,
                        nargs="+", help="需要抓取图片的链接")

    # keep args to be global space
    global args
    args = parser.parse_args()

    start_slave() if args.slave else start_master()


if __name__ == '__main__':
    main()
