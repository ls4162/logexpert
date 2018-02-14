logexpert
=============================

<br/>Log expert is a data pipeline to process clickstream from web log data in both batch and stream fashion. 
<br/>In the batch process, I conduct sessionization using UDWF(User Defined Window Function) from Spark SQL. Clickstream belongs to the same user within a time window will be defined as a session. I also label a session to be success if a payment page is requested during sessin time. User activities in each session will be consolidated as one single record in the output.
<br/>In the stream process, I try to explore the usage of sessionization in stream. The stream job maintain the session status in the past minute using updateStateByKey function from Spark Streaming. The session status also include the last clicked page, which could be used for recommendation.
<br/>The log data used is of the Apache Common Log Format. To see demo usage of the pipeline output, go to logexpert.online .

### Dashboard
![](https://drive.google.com/file/d/1TgqIuayrys2-mZi4fYaIxn-yrajeAtD3/view?usp=sharing)
![](https://drive.google.com/file/d/1VhAsNJY15EemedhvX_o96z6qqqw8cNrk/view?usp=sharing)
