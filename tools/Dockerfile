FROM gcc:10.2

RUN apt-get -y update
RUN apt-get -y install doxygen
RUN wget https://github.com/Kitware/CMake/releases/download/v3.19.0/cmake-3.19.0-Linux-x86_64.sh -q -O /tmp/cmake-install.sh && chmod u+x /tmp/cmake-install.sh
RUN /tmp/cmake-install.sh --skip-license && rm /tmp/cmake-install.sh
