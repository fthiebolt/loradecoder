#!/bin/bash
#
# Simple helper script ;)
#

if [ "$#" == "0" ]; then
    msg='update'
else
    msg="$@"
fi

set -x

#git remote set-url origin https://github.com/fthiebolt/loradecoder.git
git add --all
git commit -a -m "${msg}"
git push

