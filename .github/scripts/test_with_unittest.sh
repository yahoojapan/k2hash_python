#!/bin/sh

echo $(basename $0)

if test -f "/etc/os-release"; then
    . /etc/os-release
    OS_NAME=$ID
    OS_VERSION=$VERSION_ID
elif test -f "/etc/centos-release"; then
    echo "[OK] /etc/centos-release falling back to CentOS-7"
    OS_NAME=centos
    OS_VERSION=7
else
    echo "[NO] Unknown OS, neither /etc/os-release nor /etc/centos-release"
    exit 1
fi

echo "[OK] HOSTNAME=${HOSTNAME} OS_NAME=${OS_NAME} OS_VERSION=${OS_VERSION}"
PYTHON=""
case "${OS_NAME}-${OS_VERSION}" in
    ubuntu*|debian*)
        PYTHON=$(which python)
        ;;
    centos*|fedora*)
        PYTHON=$(which python3)
        ;;
esac

cd src

TEST_FILES="test_k2hash.py test_k2hash_package.py test_keyqueue.py test_queue.py"
for TEST_FILE in ${TEST_FILES}
do
    ${PYTHON} -m unittest k2hash/tests/${TEST_FILE}
    if test $? -ne 0; then
        echo "[NO] ${PYTHON} -m unittest k2hash/tests/${TEST_FILE}"
        exit 1
    fi
done

exit 0

