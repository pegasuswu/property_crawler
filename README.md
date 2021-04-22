# OverView
This is a simple crawling program by python. Python is so powerful in programing for crawling data in internet and this is a simple crawling program.
## Analyzing the web page
There are lots of ways and tools to analyze the web page. Here is my tools.
### chrome
Chrome is so powerful in developer mode. we can get   much information and help in chrome. By chrome I got the key url for my crawling realtor.ca.
### postman
Postman is another API tools I use to simulate the data. It is easy and convinent. By postman, I got the right datagram  format.
### pycharm
Pycharm is strongly recommended for python programming. It is powerful for debuging and tunning.

## Coding with python
After I finished the coding, I found there are so many warning in pycharm(PEP8 coding style). So,it took me about 2 hours to adjust the coding style.
### config file
There is one config file in current run directory
Here is its format
[System]
Ipaddress=127.0.0.1
Port=3306
Citynum=3
Usernum=1
[user001]
Email=1833717874@qq.com
[city001]
Name=markham
PriceMin=1000000
PriceMax=2000000
BedRange=1-0
BathRange=1-0
[city002] 
Name=toronto
PriceMin=1000000
PriceMax=2000000
BedRange=1-0
BathRange=1-0
[city003] 
Name=surrey
PriceMin=1000000
PriceMax=2000000
BedRange=1-0
BathRange=1-0
I will give the set rules related in future days. Time is so limited for me!
### crawling thread
program has a crawling thread which crawls the designated url according to the your set in configuration file. Pay attention：current version only support markham、toronto、vancouver、calgary、ottawa、edmonton、mississauge、montreal、hamilton、surrey.Other cities or regions or provinces can be added in the future or you can added information by yourself.
After crawl the new records, program will send email to you instantly.
In order to avoid the anti-crawling of realtor.ca,after every circle,the program will sleep for 2 minutes.
### The using way
It is a source program mode.if you are interested,you can modified it by yourself and you can add much more searching conditions to get your interesting results.
The program is just a practice for python crawling program and it can not be used in any commercial activities.Thanks.
