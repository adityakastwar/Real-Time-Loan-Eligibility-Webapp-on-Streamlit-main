# Real Time Loan Eligibility Prediction with ML 


In this repository I have developed an real time API to score model in real time using. For this I have hosted the app using two different ways: 
<br/>
- Flask
- Streamlit
   

<br/>
<b>Why Real time ?</b> <br/>
Because it enables your model
to be embedded in a web browser so as to get predictions and take action based on predictions.<br/>
This is useful for making online recommendations, checking credit card approval status, and so on. The application is enormous, and it becomes easier to manage such APIs using Docker.

Still, for this project  I have used historical bank data which shows wether a customer is eligible for the loan or not based on certain features.


## Technologies Used:



1:  <b>Pyspark</b>
- Used Pysaprk for its capability to process large amounts of data. 
- Trained model with RandomForstClassifer for predictions.

  
2: <b>Flask</b> 
- Used Flask to implement model in real time.
- At first I wanted to test weather the app was running successfuly or not, thats why I used flask to evaluate it.

3: <b>Docker</b> 
- Deployed the whole source code along with primiary files to the Docker. <br/>
- Docker-based deployment is useful
in a lot of ways when compared to a traditional scoring method, like spark-submit.


4: <b>Streamlit</b> 
- Build the user interface using Streamlit.
- One of the advantages of streamlit is streamlit is that you can edit the Python code and see live changes in the web app
instantly

## Implementation


To run this app on streamlit, I have created a docker-compose which is used to combine multiple Docker containers and create a
Docker network service. Running this commands will execute the docker network:<br/>

    docker build -t image
    docker-compose build


After unning the above command the docker image will tagged with some of its own name. <br/>
You need to run that command in order to execute the server.

![Alt Text](https://github.com/akhilkapil/Real-Time-Webapp-on-Streamlit/blob/main/docker.PNG)

    docker run -p realtimepredictions_streamlit:latest

And finally use this URL http://localhost:8501/  to run the app.

The app will look something like this:

![Alt Text](https://github.com/akhilkapil/Real-Time-Webapp-on-Streamlit/blob/main/app.PNG)
