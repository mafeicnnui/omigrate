#!/bin/sh
export ORACLE_SID=hist2
impdp parfile=/ops/python/script/imp.par
