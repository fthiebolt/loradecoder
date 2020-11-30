# .bashrc
#
# [dec.17] Thiebolt F.   add a different prompt when in docker
# François Sept. 15
#

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# Source local definitions
if [ -f ~/.env ]; then
    source ~/.env
fi

# User specific aliases and functions
alias rm='rm -i'
#alias cp='cp -i'
alias mv='mv -i'

alias l='ls --color -hlsv'
alias ll='ls --color -ahlsv'
alias la="ls --color -la"
#alias rm='rm -i'
#alias cp='cp -i'
alias a2ps="a2ps -q -r -2 -T 3 -A fill "
alias mpage="mpage -2 -P -c -m0 -s4 "
alias h=history
alias cls=clear
alias df="df -h"
alias fm="sync && sysctl -w vm.drop_caches=3; free -g"
alias iotop="iotop -o -P -d 3"
alias top="htop"
alias dtop="dstat --top-io-adv --top-bio-adv -d"
alias svclist="systemctl list-unit-files --type=service"
alias dmesg="dmesg -T"
alias netcon="lsof -i -n"
alias tailf="tail -f"


# History settings
export HISTSIZE=10000
export HISTTIMEFORMAT="%d-%h-%Y - %H:%M:%S "
export HISTCONTROL=erasedups
shopt -s histappend

# Local 'bin' directory
if [ -d ${HOME}/bin ]; then
	export PATH=${HOME}/bin:${PATH}
fi

# My beautifull prompt :)
export PS1="\[\e[7m\]\u@\h\[\e[m\][\W] "

# Docker mode
if [[ $(cat /proc/1/sched 2>/dev/null | head -1) != systemd* && $(cat /proc/1/comm 2>/dev/null | head -1) != systemd* ]]; then 
    export PS1="\[\e[37;44m\]\u@\h\[\e[m\][\W] "
    # inherit env. var from PID 1
    . <(xargs -0 bash -c 'printf "export %q\n" "$@"' -- < /proc/1/environ)
fi

# Various defs
export EDITOR=vim

