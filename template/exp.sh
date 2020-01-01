#!/bin/sh
export ORACLE_SID=@SID
expdp parfile=@PATH/script/exp.par
