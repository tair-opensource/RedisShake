import os
import getopt
import sys

def usage():
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print('| Usage: ./run_direct.py --src=source_redis_address --srcPasswd=source_password --srcType=source_type(default is \'standalone\') --dst=target_redis_address --dstPasswd=target_redis_address --dstType=target_redis_address(default is \'standalone\') |')
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print('| Like : ./run_direct.py --src=10.1.1.1:3456 --srcPasswd=Test123456 --srcType=standalone --dst=20.1.1.2:15678 --dstPasswd=Test123456 --dstType=standalone |')
    print('| Like : ./run_direct.py --src=10.1.1.1:3456;10.1.1.2:5678;10.1.1.3:7890 --srcPasswd=Test123456 --srcType=standalone --dst=20.1.1.1:13456;20.1.1.2:15678 --dstPasswd=Test123456 --dstType=proxy |')
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    exit(0)

if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "hs:a:t:d:p:e:", ["help", "src=", "srcPasswd=", "srcType=", "dst=", "dstPasswd=", "dstType="])
    if len(opts) == 0:
        usage()

    mp = {}
    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
            sys.exit()

        if key in ("-s", "--src"):
            mp['source.address'] = value
        if key in ("-a", "--srcPasswd"):
            mp['source.password_raw'] = value
        if key in ("-t", "--srcType"):
            mp['source.type'] = value
        if key in ("-d", "--dst"):
            mp['target.address'] = value
        if key in ("-p", "--dstPasswd"):
            mp['target.password_raw'] = value
        if key in ("-e", "--dstType"):
            mp['target.type'] = value

    mp['id'] = 'redis-shake'
    mp['source.type'] = 'standalone' if 'source.type' not in mp else mp['source.type']
    mp['target.type'] = 'standalone' if 'target.type' not in mp else mp['target.type']
    mp['source.auth_type'] = 'auth'
    mp['target.auth_type'] = 'auth'
    mp['rewrite'] = 'true'
    mp['log.file'] = 'redis-shake.log'

    name = "run_direct.conf"
    f = open(name, "w+")
    for key, val in mp.items():
        f.writelines('%s = %s\n' % (key, val))
    f.close()

    os.system("./redis-shake.linux -type=sync -conf=%s" % name)
    #os.system("./redis-shake.darwin -type=sync -conf=%s" % name)
