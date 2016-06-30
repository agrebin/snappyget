#!/usr/bin/python
#########################################################################
# Author: Adrian Grebin (agrein@digitalriver.com)                       #
# Tool to merge  multiple snappy files in hdfs topics, with date format #
#########################################################################
from snakebite.client import Client
import hdfs
from dateutil.parser import parse
from datetime import *
import os
#sys.tracebacklimit=1

#
#  This class is to handle multiple HDFS aqueduct topic itasks
#
class HDFS_topic(object):
	def __init__(self,topic,user,server,port,web_port,base,hdfs_tmp):
		self.topic = topic
		self.username = user
		self.server = server
		self.port = port
		self.base = base
		self.path = ["%s/%s" % (base,topic)]
		self.hdfs_tmp = hdfs_tmp

		try:
			self.client=Client(server,port,effective_user=user) 
			self.hdfsclient=hdfs.client.InsecureClient(\
                 "http://%s:%d" % (server,web_port),user=user)
	 		self.daylist=self.check()
		except:
	 		print "Base path %s does not contain valid structure" % (base)
	 		raise	
	
	#
	# Check basic hdfs access and that directory format is appropiate
	# also builds datelist structure
	#
	def check(self):
		self.content=self.client.ls(self.path)
		ret=[]
		for item in self.content:
			(head,tail) = os.path.split(item['path'])
			try:
				parse(tail,yearfirst=True,dayfirst=True)
				if item['file_type'] == 'd':
					ret.append(tail)
				else:
					print("WARNING: %s is not a directory, skipping\n" % (item['path']))
			except:
				print("WARNING: %s is not in date format, skipping\n"  % (tail))

		if len(ret) > 0:
			ret.sort(key=lambda x: datetime.strptime(x,"%Y-%m-%d"))
			return ret
		else:
			return false

	#
	# Give a date, check if that date is on the dirlist and return matching dir entry
	#
	def day_in_topic(self, date):
		for item in self.daylist:
			if parse(date) == parse(item):
				return item
		return False


	#
	# Check and validates date_from and date_to arguments
	#	
	def check_date_range(self,date_from,date_to):
		if date_from:
			try:
				parse(date_from)
			except:
				raise ValueError("FATAL: start date (%s) invalid date format" % (date_from) )
		
			if ( parse(date_from)  < parse(self.daylist[0])  ) or ( parse(date_from)  > parse(self.daylist[-1]) ):
				raise ValueError("FATAL: start date (%s) not in range (%s ---> %s)" % (date_from,self.daylist[0],self.daylist[-1]))
			else:
				ret_from=parse(date_from).strftime("%Y-%m-%d") 
				while not self.day_in_topic(ret_from):
					print "WARNING: start date %s not in topic %s, trying next day" % (ret_from,self.topic)
					ret_from=datetime.strftime((parse(ret_from)+timedelta(days=1)), "%Y-%m-%d" )
					
				ret_from=self.day_in_topic(ret_from)

				
		else:
				ret_from=self.daylist[0]

		if date_to:
			try:
				parse(date_to)
			except:
				raise ValueError("FATAL: end date (%s) invalid date format" % (date_to) )

			if ( parse(date_to)  < parse(self.daylist[0])  ) or ( parse(date_to)  > parse(self.daylist[-1]) ):
				raise ValueError("FATAL: end date (%s) not in range (%s ---> %s)" % (date_to,self.daylist[0],self.daylist[-1]))
			else:
				ret_to=parse(date_to).strftime("%Y-%m-%d")
		else:
				ret_to=self.daylist[-1]
		
		if (parse(ret_from) > parse(ret_to) ):
			raise ValueError("FATAL: start date (%s) must be <= end date (%s)" % (ret_from,ret_to))


		return (ret_from,ret_to)
		
	
	#
	#  Traverses the list of valid directories and merges each day
	#
	def merge(self,date_from="",date_to=""):
		day=""
		try:
			(day,date_to)=self.check_date_range(date_from,date_to)
		except Exception as err:
			raise ValueError(err)

		print "INFO: Trying to merge %s from %s to %s\n" % (self.topic,day, date_to)

		while (parse(day) <= parse(date_to)):
			if  self.day_in_topic(day):
				self.merge_day(day)
			else:
				print "WARNING: %s is not on %s, skipping\n" % (day,self.path)

			day=datetime.strftime((parse(day)+timedelta(days=1)), "%Y-%m-%d" )
			while not self.day_in_topic(day) and parse(day) <= parse(date_to):
				print "WARNING: %s not found in %s, trying next day" % (day,self.topic)
				day=datetime.strftime((parse(day)+timedelta(days=1)), "%Y-%m-%d" )

			day=self.day_in_topic(day)
			if not day:
				return	
				
		return True

	#
	# Given a date, if there are files that are not .snappy download and remove them, then getmerge, and upload everything
	#
	def merge_day(self,date):
		print "INFO: processing ", date
		daytmp="%s/snappymerge-%s-tmp" % (self.hdfs_tmp,date)
		daypath=["%s/%s/%s/" % (self.base, self.topic,date)]
		#mergedfile="./%s-merged.snappy" % (date)
		mergedfile="./%s-merged.snappy" % (datetime.strftime(datetime.now(),"%Y-%d-%m.%f"))
		day_files=[x['path'] for x in self.client.ls(daypath)]
		print "INFO: DAYPATH: ", daypath
		try:
			os.remove(mergedfile)
		except:
			pass

		
		if len([ x for x in day_files if x.endswith('.snappy') ]) <= 1:
			print "WARNING: %s does not have enough files to getmerge, skipping" % (date)
			return

		if [ file for file in day_files if not file.endswith('.snappy') ]:
				print "WARNING: %s contains a non snappy file (%s), moving *snappy to %s getmerge there\n" % (daypath,file,daytmp)
				self.merge_with_move(daypath[0],daytmp,day_files,mergedfile)
		else:
			print "INFO: MERGING ", daypath[0]
			result=self.client.getmerge(daypath[0],mergedfile)
			print [x for x in result if not x['result']]
		
			print "INFO: DELETING original files in ", daypath[0]
			for file in day_files:
				print "INFO: Deleting original file ", file
				self.hdfsclient.delete(file)

			print "INFO: UPLOADING merged (%s) to %s" % (mergedfile,daypath[0])
			self.hdfsclient.upload(daypath[0],mergedfile,overwrite=True)
			os.remove(mergedfile)

		return

#
# When there are files that do not contain .snappy suffix, merge with move, first moves everyting to an hdfs temp dir, merges there, and uploads
#
	def merge_with_move(self,day_path,day_tmp,dayfiles,merged_file):
		self.hdfsclient.makedirs(day_tmp)


		print "INFO: MOVING files to ", day_tmp
		snap = [x for x in dayfiles if x.endswith(".snappy")]
		result=self.client.rename(snap,day_tmp)
		print [ x['path'] for x in result if not x['result']]

		print "INFO: MERGING files in ", day_tmp
		result=self.client.getmerge(day_tmp,merged_file)
		print [x['path'] for x in result if not x['result']]

		print "INFO: UPLOADING merged (%s) to %s"  % (merged_file,day_path)
		self.hdfsclient.upload(day_path,merged_file,overwrite=True)
		os.remove(merged_file)

		print "INFO: Deleting files on ", day_tmp
		self.hdfsclient.delete(day_tmp,recursive=True)


				
if __name__ == '__main__' :
	import argparse

	count=0

	parser = argparse.ArgumentParser(description="Merge daily historical snappy files into one to save hdfs space")
	parser.add_argument('topic', help="Topic name relative to --base")
	parser.add_argument('--hdfs_user', help="HDFS user name (default: current user)",default=None)
	parser.add_argument('--hdfs_server', help="HDFS server name or ip (default: aquhmstsys022001.c022.digitalriverws.net)",default="aquhmstsys022001.c022.digitalriverws.net")
	parser.add_argument('--hdfs_port', help="HDFS server port number (default:8020)", type=int, default=8020)
	parser.add_argument('--hdfs_tmp', help="HDFS temporary dir to store files to be merged (default:/user/hduser/tmp)", default="/user/hduser/tmp")
	parser.add_argument('--web_port', help="HDFS server WEB port number (default:50070)", type=int, default=50070)
	parser.add_argument('--base', help="Alternate hdfs base path for topic (default:/user/aqueduct/flume)",default="/user/aqueduct/flume")
	parser.add_argument('--start', help="Start Date inclusive  (default: from beginning)")
	parser.add_argument('--end', help="End Date inclusive (default: to end)")


	args = parser.parse_args()
	topic=HDFS_topic(topic=args.topic,user=args.hdfs_user,server=args.hdfs_server,port=args.hdfs_port,\
                     hdfs_tmp=args.hdfs_tmp,web_port=args.web_port,base=args.base)
	try:
		topic.merge(args.start,args.end)
	except Exception as err:
		print err
		exit

