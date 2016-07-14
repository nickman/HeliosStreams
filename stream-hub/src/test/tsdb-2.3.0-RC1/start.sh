#!/bin/bash
OHOME=/home/nwhitehead/services/opentsdb/opentsdb-2.3.0-RC1
CMDS="$OHOME/tsdb tsd --config=$OHOME/opentsdb.conf --staticroot=$OHOME/staticroot --cachedir=/tmp/tsdb"
$CMDS