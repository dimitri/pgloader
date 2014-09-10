
# GnuPG Agent
export GPG_TTY=`tty`
eval "$(gpg-agent --daemon)"

# DEBIAN
export DEBEMAIL="dim@tapoueh.org"
export DEBFULLNAME="Dimitri Fontaine"
export DEBSIGN_KEYID="60B1CB4E"
