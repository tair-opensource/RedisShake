FROM busybox

COPY ./bin/redis-shake /usr/local/app/redis-shake
COPY ./conf/redis-shake.conf /usr/local/app/redis-shake.conf
ENV TYPE sync
CMD /usr/local/app/redis-shake -type=${TYPE} -conf=/usr/local/app/redis-shake.conf
