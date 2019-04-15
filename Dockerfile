FROM centos

RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm
RUN yum install -y python36u python36u-devel python36u-pip

RUN ln -sfv /usr/bin/python3.6 /usr/bin/python3
RUN ln -sfv /usr/bin/pip3.6 /usr/bin/pip3

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY main.py /usr/local/

WORKDIR /usr/local/
CMD ["python3.6","-u","main.py"]
