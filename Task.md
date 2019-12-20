
# Task

In the mobile telephone network there are lots of devices that need to work together to give the customer a pleasant experience. 
Examples of those devices is a customers cell phone and so called cells which the phone connects to. 
Cells can have different features and have gone through evolution. 
There are cells of different technology 
- gsm (2g) 
- umts (3g)
- lte (4g)

Cells are grouped into physical locations called sites. 
One site has at least one cell and can be composed of any mix of cells in different technologies. 
At Telia we receive a snapshot of this cell/site data every day for all the countries.

Each cell of a technology can operate on different frequency bands. 
For 
- **gsm** there are 900 and 1800 MHZ, 
- **umts** has 900 and 2100 MHZ and 
- **lte** has 700, 800, 2600, 1800 and 2100 MHZ.

Your task is to take the data in csv format and write a spark application based on DataFrames that analyses the data. 
The data has 2 parts. 

The first part is the cell data (gsm.csv, umts.csv and lte.csv) and 
the second one is the site data (site.csv). 

The data, as mentioned before, is organised by day. Your task is to create one data set for all cells and enrich each cell with information from the site. 
All cells that cannot be assigned a site should be dropped.

The calculated values should be the following:
1. How many cells per technology are there on that site. The column name should be as follows: site_$TECHNOLOGY_cnt
	- a. Example: for a site S there are 2 cells gsm, 3 cells umts and 0 cells lte. 
	- The result should be site_4g_cnt: 0, site_3g_cnt: 3, site_2g_cnt: 2

2. Which technology bands do exist on a site S
	- a. Example: for a site S there is 2 cell gsm with 900MHZ, one cell umts 2100MHZ and one cell lte with 1800MHZ. 
	- The result should be: frequency_band_G900_by_site: 1devises, frequency_band_U2100: 1, frequency_band_L1800: 1, frequency_band_L2100: 0 (and so on)

3. The result is to be saved into some configurable directory. 
	- The granularity of the input data set is to be kept.

That concludes the code task. 
In addition please provide a readme on how your task is configured and run. 
Include some thoughts about the lifecycle of the application. 
That should include steps which are part of the process of putting code into production. 
Examples of what that might include are: 
	How would you schedule your code to run every day? 
	How would you handle configuration? 
	How do you make sure refactoring in the future will not break your logic? 
	Did you see something strange in the data?

