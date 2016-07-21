# python cli for qcloud cos

没什么说的，这么简单的东西……

算了还是简单说一下吧……

CLI:

    ./cosfs

    ./cosfs ls /

    ./cosfs cp /etc/hosts cos:/

    ./cosfs ls / -l

    ./cosfs mkdir /test
    ./cosfs cp /etc/hosts cos:/test/
    ./cosfs cp cos:/hosts /tmp/hosts

    ./cosfs ls / -r
    ./cosfs ls / -rl

    ./cosfs cpdir ./foo  cos:/test/     # 'cp -r ./foo cos:/test/'
    ./cosfs cpdir ./foo/ cos:/test/ -v  # 'cp -v -r ./foo/* cos:/test/'

    ./cosfs rmdir /test/
    ./cosfs rmdir /test/ -r

    ./cosfs cat /hosts
    ./cosfs mv /hosts /hosts.bak

    ./cosfs rm /hosts.bak

SDK:

    import CosFS
