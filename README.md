# python cli for qcloud cos

注：目前只兼容 python 2.x，不支持 3.x

没什么说的，这么简单的东西……

算了还是简单说一下吧……

command list:

    stat    显示cos文件大小和修改时间等
    mv      在cos上移动文件
    ls      列出目录、文件（支持*前缀匹配）
    cpdir   从本地传目录到cos（暂不支持cos到本地）
    rm      删除cos文件
    cat     输出cos文件内容
    cp      从本地拷贝文件到cos，或从cos拷贝回来
    mkdir   在cos创建目录
    rmdir   删除cos目录

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
