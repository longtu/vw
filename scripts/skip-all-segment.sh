#!/usr/bin/env bash

array=$(hadoop fs -ls /user/flag | sed 1d | perl -wlne'print +(split " ",$_,8)[7]' )

for entry in ${array}
do
	hadoop fs -rm -r "$entry"
	hadoop fs -mkdir "$entry"
	hadoop fs -touchz "$entry"/${1}
done
