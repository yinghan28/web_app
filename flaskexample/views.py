from flask import request
from flask import render_template
from flaskexample import app
from prediction_random_forest import delay_prob_prediction
from airport_location import airport_lat_long
from datetime import datetime


@app.route('/')
@app.route('/input')
def cesareans_input():
    return render_template("input.html")

@app.route('/output')
def cesareans_output():
    #input from input.html
    origin = request.args.get('from').upper()
    dest = request.args.get('to').upper()
    date = request.args.get('date')
    time = request.args.get('time')
    depOrArr = request.args.get('DepOrArr')
    checked = 'check' in request.form
    print depOrArr,checked
    if depOrArr!="Arrival":
        depOrArr = "Departure"
    print origin, dest, date, time, depOrArr
    inputs = {'origin':origin,'dest':dest,'date':date,'time':time,'depOrArr':depOrArr}
    try:
    	# format date as 'yyyy-mm-dd'
    	month, day, year = date.split('/')
    	month_padded = '0' * (2 - len(month)) + month
    	day_padded = '0' * (2 - len(day)) + day
    	year_padded = '20' * ((4 - len(year)) / 2) + year
    	date_formatted = '-'.join([year_padded, month_padded, day_padded])

    	# format time as hour in 0-23
    	if time[-2:].lower() == 'pm':
        	hour = str(int(time[:-2]) + 12)
    	elif time[-2:].lower() == 'am':
        	hour = time[:-2]
    	else:
        	hour = time

    	results = delay_prob_prediction(origin, dest, date_formatted, hour,
                                    depOrArr)                        
    	#output1
    	flights = []
    	for index in range(results.shape[0]):
        	result = results.ix[index, :]
            
        	dict = {'airline' : airline_code2name(result['Carrier']),
                	'no' : result['Carrier'] + ' ' + result['FlightNumber'],
                	'from' : result['Origin'],
                	'to' : result['Dest'],
                	'departure' : result['Dep_datetime'],
               		'arrival' : result['Arr_datetime'],
                    'delay' : parse_delay(result['ProbDelay']),
                	'delay_grade': parse_delay_grade(result['ProbDelay']),
                	'bar_width':parse_delay_to_bar_length(result['ProbDelay']),
                	'container' : 'container' + str(index+1)
                	}
        	flights.append(dict)


    	#output2
    	olat, olong = airport_lat_long(origin)
    	dlat, dlong = airport_lat_long(dest)
    	airports = {'origin':origin,'dest':dest,'olat':olat,'olong':olong,'dlat':dlat,'dlong':dlong}
    	print airports['origin']
	print airports
    except:
	flights = "error"
	airports = {'origin':'wrong','dest':'wrong','olat':'wrong','olong':'wrong','dlat':'wrong','dlong':'wrong'}
    	print airports
	print airports['origin']
    return render_template("output.html", airports=airports, flights=flights, inputs=inputs)

verylow_cutoff = 0.51
low_cutoff = 0.56
medium_cutoff = 0.7
high_cutoff = 0.73

def parse_delay(delay_prob):
    if delay_prob < verylow_cutoff:
        return "Minimal risk"
    elif delay_prob < low_cutoff:
        return "Below average risk"
    elif delay_prob < medium_cutoff:
        return "Average risk"
    elif delay_prob < high_cutoff:
        return "Above average risk"
    else:
        return "High risk"

def parse_delay_grade(delay_prob):
  if delay_prob < verylow_cutoff:
    return 0
  elif delay_prob < low_cutoff:
    return 1
  elif delay_prob < medium_cutoff:
    return 2
  elif delay_prob < high_cutoff:
    return 3
  else:
    return 4

def parse_delay_to_bar_length(delay_prob):
  if delay_prob < verylow_cutoff:
    return 0.05
  elif delay_prob < low_cutoff:
    return 0.25
  elif delay_prob < medium_cutoff:
    return 0.5
  elif delay_prob < high_cutoff:
    return 0.75
  else:
    return 1.0

def airline_code2name(code):
    airline_dict = {'HA' : 'Hawaiian',
                    'DL' :  'Delta',
                    'EV' :  'ExpressJet',
                    'US' :  'US Airways',
                    'AS' :  'Alaska',
                    'NK' :  'Spirit',
                    'VX' :  'Virgin America',
                    'MQ' :  'Envoy Air',
                    'B6' :  'JetBlue',
                    'OO' :  'SkyWest',
                    'UA' :  'United',
                    'WN' :  'Southwest',
                    'F9' :  'Frontier',
                    'AA' :  'American',
                    }
    return(airline_dict[code])
