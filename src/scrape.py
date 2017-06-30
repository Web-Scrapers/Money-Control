import requests
from bs4 import BeautifulSoup
import copy
import re
import os
import json

# Libraries required to limit the time taken by a request
import signal
from contextlib import contextmanager

baseurl		= "http://www.moneycontrol.com"
base_dir	= "../output"
company_dir	= base_dir+'/Companies'
category_Company_dir = base_dir+'/Category-Companies'
company_sector = {}

class TimeoutException(Exception): pass

@contextmanager
def time_limit(seconds):
	def signal_handler(signum, frame):
		raise TimeoutException
	signal.signal(signal.SIGALRM, signal_handler)
	signal.alarm(seconds)
	try:
		yield
	finally:
		signal.alarm(0)


def ckdir(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)
	return


def get_response(aurl):
	hdr				= {'User-Agent':'Mozilla/5.0'}

	while True:
		try: 
			# Waiting 60 seconds to recieve a responser object
			with time_limit(30):
				content 				= requests.get(aurl,headers=hdr).content
			break
		except Exception:
			print("Error opening url!!")
			continue

	return content

# Procedure to return a parseable BeautifulSoup object of a given url
def get_soup(aurl):
	response 		= get_response(aurl)
	soup 			= BeautifulSoup(response,'html.parser')
	return soup


def get_categories(aurl):
	soup	= get_soup(aurl)
	links = {}
	tables	= soup.find('div',{'class':'lftmenu'})
	categories = tables.find_all('li')
	for category in categories:
		category_name = category.get_text()
		if category.find('a',{'class':'act'}):
			links[category_name] = aurl
		else:
			links[category_name] = baseurl + category.find('a')['href']
	return links


def get_values(soup,fname):
	data	= soup.find_all('table',{'class':'table4'})
	rows	= data[1].find_all('tr')
	final_rows = []
	flag 	= 1
	while flag == 1:
		flag = 0
		final_rows = []
		for row in rows:
			if row.find('tr'):
				flag = 1
				inner_rows = row.find_all('tr')
				for inner_row in inner_rows:
					final_rows.append(inner_row)
			else:
				final_rows.append(row)

		rows = copy.copy(final_rows)

	rows = []

	with open(company_dir+'/'+fname,'w') as outfile:
		for i in range(0,len(final_rows)):
			if final_rows[i]['height'] == '1px':
				break

			fields = final_rows[i].find_all('td')

			fields[0] = re.sub(',','/',fields[0].get_text())
			for i in range(1,len(fields)):
				fields[i] = re.sub(',','',fields[i].get_text())

			for field in fields[:-1]:
				outfile.write(field+",")
			outfile.write(fields[-1] + "\n")

			# for field in fields[:-1]:
			# 	value = field.get_text()
			# 	# value = re.sub(',' , '|', value)
			# 	outfile.write(value + ",")
			# outfile.write(fields[-1].get_text() + "\n")
			# # rows.append(row)

	return


def get_Data(aurl,fname):
	soup	= get_soup(aurl)
	og_table	= soup.find('div',{'class':'boxBg1'})
	links	= og_table.find('ul',{'class':'tabnsdn FL'})
	for link in links.find_all('li'):
		format_type = link.get_text()
		new_fname = fname + format_type + ".csv"
		if link.find('a',{'class':'active'}):
			table 		= og_table
		else:
			web_address	= baseurl + link.find('a')['href']
			new_soup	= get_soup(web_address)
			table 		= new_soup.find('div',{'class':'boxBg1'})

		get_values(table,new_fname)

	return


def get_PL_Data(aurl,aname):
	get_Data(aurl,aname+"-PL-")
	return


def get_BS_Data(aurl,aname):
	rows = get_Data(aurl,aname+"-BS-")
	return


def get_sector(asoup):

	sector = None

	try:
		details = asoup.find('div',{'class':'FL gry10'})
		headers = details.get_text().split('|')
	except AttributeError:
		return sector

	for header in headers:
		if "SECTOR" in header:
			# print(header.split(':')[1].strip())
			sector = header.split(':')[1].strip()
			break

	return sector

def get_Company_Data(aurl,aname):
	soup	= get_soup(aurl)


	temp 		= soup.find('dl',{'id':'slider'})
	try:
		links		= temp.find_all(['dt','dd'])

	except AttributeError:
		print("Data on '"+aname + "' doesn't exist anymore.")
		return

	index 		= -1
	for i in range(0,len(links)):
		if links[i].get_text() == 'FINANCIALS':
			index = i
			break
	# something
	if index != -1:
		fields 	= links[i+1].find_all('a')

		required_link = None
		for field in fields:
			if field.get_text() == "Profit & Loss":
				required_link = baseurl + field['href']
				get_PL_Data(required_link,aname)

			if field.get_text() == "Balance Sheet":
				required_link = baseurl + field['href']
				get_BS_Data(required_link,aname)

	company_sector["companies"][aname] = get_sector(soup)

	with open(base_dir+"/company-sector.json",'w') as outfile:
		json.dump(company_sector,outfile)
	return


def get_list(aurl,category):
	details	= []
	soup	= get_soup(aurl)
	filters	= soup.find_all('div',{'class':'MT10'})
	table	= filters[3].find_all('div',{'class':'FL'})[2]
	rows 	= table.find_all('tr')
	headers= rows[0].find_all('th')
	labels	= {}
	for i in range(0,len(headers)):
		labels[i] = headers[i].get_text()

	for row in rows[1:]:
		company = {}
		fields = row.find_all('td')
		for i in range(0,len(headers)):
			company[labels[i]] = fields[i].get_text()
		company['link'] = baseurl + fields[0].find('a')['href']
		get_Company_Data(company['link'],company['Company Name'])
		details.append(company)

	with open(category_Company_dir+'/'+category+'.json','w') as outfile:
		json.dump({'Company_details':details},outfile)


def get_sector_data(aurl):
	# categories = get_categories(aurl)

	# with open(base_dir+'/categories.json','w') as outfile:
	# 	json.dump(categories,outfile)

	with open(base_dir+"/categories.json",'r') as infile:
		categories = json.load(infile)

	category = "Utilities"

	category_url = categories[category]

	print("Accessing companies. Category : "+category)

	company_list	= get_list(category_url,category)


def get_alpha_quotes(aurl):
	soup = get_soup(aurl)

	print(aurl)

	list = soup.find('table',{'class':'pcq_tbl MT10'})

	companies = list.find_all('a')

	for company in companies[:]:
		if company.get_text() != '':
			print(company.get_text()+" : "+company['href'])
			get_Company_Data(company['href'],company.get_text())


def get_all_quotes_data(aurl):
	soup = get_soup(aurl)
	list = soup.find('div',{'class':'MT2 PA10 brdb4px alph_pagn'})

	links= list.find_all('a')

	for link in links[8:]:
		# print(link.get_text()+" : "+baseurl+link['href'])
		print("Accessing list for : "+link.get_text())
		get_alpha_quotes(baseurl+link['href'])

if __name__ == '__main__':
	sector_url		= 'http://www.moneycontrol.com/india/stockmarket/sector-classification/marketstatistics/nse/automotive.html'
	quote_list_url 	= 'http://www.moneycontrol.com/india/stockpricequote'

	url 			= quote_list_url

	print("Initializing")
	ckdir(base_dir)
	ckdir(company_dir)
	ckdir(category_Company_dir)
	try:
		with open(base_dir+"/company-sector.json",'r') as infile:
			company_sector = json.load(infile)
	except FileNotFoundError:
		company_sector = {"companies":{}}

	# print(company_sector)

	# get_sector_data(url)
	get_all_quotes_data(url)