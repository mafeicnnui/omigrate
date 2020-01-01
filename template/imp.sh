#!/bin/sh
export ORACLE_SID=@SID
impdp parfile=@PATH/script/imp.par
