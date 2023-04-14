#!/bin/sh

case `uname -srm` in
  Darwin?1[012345]*)
    bin=$here/bin/sys-i386-snow-leopard
	SED="sed -E"
	echo "supported"
    ;;
  Darwin?1[6789]*)
    bin=$here/bin/sys-x86-64-sierra
	SED="sed -E"
	echo "supported"
    ;;
  Darwin?2*)
    bin=$here/bin/sys-x86-64-sierra
	SED="sed -E"
	echo "supported"
    ;;
  Linux*x86_64*)
    bin=$here/bin/sys-x86-64-el7
	SED="sed -r"
	echo "supported"
    ;;
  *)
    echo "unrecognised platform `uname -srm`" >&2
    echo "edit scripts/setup, or set LXPATH to appropriate path" >&2
    exit 1

esac