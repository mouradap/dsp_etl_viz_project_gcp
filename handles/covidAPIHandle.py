import requests


class CovidAPIHandle:
    def __init__(self):
        self.base_url = "https://covid19-update-api.herokuapp.com/api/v1/"

    def get_response(self, req):
        print("Making a request for: {0}{1}".format(self.base_url, req))
        response = requests.get(f"{self.base_url}{req}")
        print("Request response: {}".format(response.status_code))

        return response


if __name__ == "__main__":
    api = CovidAPIHandle()
    response = api.get_response("world")
    print(response.json())
