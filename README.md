# python cli for qcloud cos

注：目前只兼容 python 2.x，不支持 3.x

没什么说的，这么简单的东西……

算了还是简单说一下吧……

CLI:

    ./cosfs

    ./cosfs ls /

    ./cosfs cp /etc/hosts cos:/

    ./cosfs ls / -l
    ./cosfs ls /test* -l            #前缀匹配查询

    ./cosfs mkdir /test
    ./cosfs cp /etc/hosts cos:/test/
    ./cosfs cp cos:/hosts /tmp/hosts

    ./cosfs ls / -r
    ./cosfs ls / -rl

    ./cosfs cpdir ./foo  cos:/test/ # 'cp -v -r ./foo cos:/test/'
    ./cosfs cpdir ./foo/ cos:/test/ # 'cp -v -r ./foo/* cos:/test/'
    ./cosfs cpdir ./foo/ cos:/test/ -i # ignore file that already exists @ cos

    ./cosfs rmdir /test/
    ./cosfs rmdir /test/ -r

    ./cosfs cat /hosts
    ./cosfs mv /hosts /hosts.bak

    ./cosfs rm /hosts.bak

SDK:

    import CosFS
