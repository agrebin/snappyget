#!/bin/bash
# Script to quickly create a mock dir to test
hdfs dfs -rm -R  hdfs://aquhmstsys022001.c022.digitalriverws.net:8020/user/hduser/gc_rum/*
hdfs dfs -cp hdfs://aquhmstsys022001.c022.digitalriverws.net:8020/user/hduser/mock/* hdfs://aquhmstsys022001.c022.digitalriverws.net:8020/user/hduser/
