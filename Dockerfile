FROM astronomerinc/ap-airflow:0.8.2-1.10.2-onbuild

#########################################################
# Building native dependencies for LightGBM module
#########################################################
RUN set -x && \
    echo "" && \
    echo "***** Building native dependencies for LightGBM module *****" && \
    apk update && \
    apk --no-cache add libstdc++ && \
    apk --no-cache add --virtual .builddeps \
        build-base \
        ca-certificates \
        cmake \
        wget && \
    git clone --recursive https://github.com/Microsoft/LightGBM ; cd LightGBM && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j4 && \
    make install && \
    pip3 install lightgbm


#########################################################
# Building native dependencies for XGBoost module
#########################################################
RUN set -x && \
    echo "" && \
    echo "***** Building native dependencies for XGBoost module *****" && \
    apk add --update --no-cache --virtual=.build-dependencies git && \
    apk add --update --no-cache --virtual=.build-dependencies make gfortran python3-dev py-setuptools g++ && \
    apk add --no-cache openblas lapack-dev libexecinfo-dev libstdc++ libgomp && \
    ln -s /usr/include/locale.h /usr/include/xlocale.h && \
    pip install numpy==1.13.3 && \
    pip install scipy==1.0.0 && \
    pip uninstall -y enum34 && \
    pip install pandas==0.22.0 scikit-learn==0.19.1 && \
    mkdir /src && \
    cd /src && \
    git clone --recursive -b v0.81 https://github.com/dmlc/xgboost && \
    sed -i '/#define DMLC_LOG_STACK_TRACE 1/d' /src/xgboost/dmlc-core/include/dmlc/base.h && \
    sed -i '/#define DMLC_LOG_STACK_TRACE 1/d' /src/xgboost/rabit/include/dmlc/base.h && \
    cd /src/xgboost/ && \
    ./build.sh && \
    #ln -s locale.h /usr/include/xlocale.h && \
    #cd /src/xgboost; make -j4 && \
    cd /src/xgboost/python-package && \
    python3 setup.py install && \
    rm /usr/include/xlocale.h && \
    rm -r /root/.cache && \
    rm -rf /src && \
    apk del .build-dependencies


#########################################################
# Installing Tables and H5py which are needed by Tensorflow
#########################################################

RUN set -x && \
    pip3 install https://github.com/better/alpine-tensorflow/releases/download/alpine3.7-tensorflow1.7.0/tensorflow-1.7.0-cp36-cp36m-linux_x86_64.whl
