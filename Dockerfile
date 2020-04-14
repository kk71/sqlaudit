# Author: kk.Fang(fkfkbill@gmail.com)

FROM python:3.8
MAINTAINER bill

WORKDIR /project
COPY requirements.txt /tmp
COPY files /tmp/
RUN     pip install --no-cache-dir -r /tmp/requirements.txt --index-url https://pypi.doubanio.com/simple/ --trusted-host pypi.doubanio.com && \
        apt-get update && \
        apt-get install libaio1 && \
        apt-get clean && \
        unzip /tmp/instantclient-basic-linux.x64-18.5.0.0.0dbru.zip -d /root/ && \
        mv /root/instantclient_18_5 /root/instantclient
ENV ORACLE_HOME=/root/instantclient
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME:/lib:/usr/lib:~/lib
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
CMD ["python", "-m http.server"]
