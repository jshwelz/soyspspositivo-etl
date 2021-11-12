import requests




def main():
    URL = 'https://alexandria.sandbox.indeed.net/api/2/imhotep-indexes/adclick2'
  
    # location given here
    location = "delhi technological university"
  
    # defining a params dict for the parameters to be sent to the API
    PARAMS = {'address':location}
  
    # sending get request and saving the response as response object
    lis = []
    

    try:
        re = requests.get(url = URL, headers={'Authorization': 'Basic aWFtX2p3bGVjaGV6OldvcmxkMTE4NyE='})
        data = re.json()
        team = data['owner']
        ddo = data['ddoFullName']
    except Exception as ex:
        print(ex)
    n = 1


if __name__ == '__main__':
    main()