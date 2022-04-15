## Running locally

To execute the project locally, a docker compose structure is defined in which is available a service for API.

Run API with compose

```
cd /path/to/the/project/integration-skill-test-server
docker-compose up --build
```
the API will be available in 0.0.0.0:5000

## Using Heroku
### Getting started

Make sure you have a working Docker installation (eg. docker ps) and that you’re logged in to Heroku (heroku login).

Log in to Container Registry:

```
¢ heroku container:login
```

Navigate to the app’s directory and create a Heroku app:
```
$ cd /path/to/the/project/integration-skill-test-server
```

If you don't have a Heroku app create a new one:
```
$ heroku create
Creating salty-fortress-4191... done, stack is heroku-18
https://salty-fortress-4191.herokuapp.com/ | https://git.heroku.com/salty-fortress-4191.git
```

if you already have a Heroku app add the remote:
```
$ heroku git:remote -a {heroku_app_name}
```

Set config vars
```
$ heroku config:set CLIENT_ID=mRkZGFjM CLIENT_SECRET=ZGVmMjMz TOKEN_VALUES=AYjcyMzY3ZDhiNmJkNTY RICHARD_ID=ae9c81fe-163e-4546-8349-19dbf63715c7 BEAUTY_ID=9001976c-a9e7-4b95-b133-9ac8ba213fb2
``` 

Build the image and push to Container Registry:
```
$ heroku container:push web
```
Then release the image to your app:
```
$ heroku container:release web
```
Now open the app in your browser:

```
$ heroku open
```

## Testing server
This API is available using a Heroku server in the following link https://integration-skill-test-server.herokuapp.com/

## Docs

You can find the API docs in the directory `docs`. Also the docs are available in the following link https://documenter.getpostman.com/view/1992239/TVmMgxxp