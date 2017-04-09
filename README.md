# tweetsdemo-storm

Sample application to stream tweets and do word count of tweets. It also persists tweets into HDFS

### Build
mvn clean pakcage

### Deploy
storm jar storm-twitter-word-count-jar-with-dependencies.jar com.ninja.demo.Topology -Dtwitter4j.debug=true -Dtwitter4j.oauth.consumerKey=xxxxxxxxxxxxxxxxxx -Dtwitter4j.oauth.consumerSecret=xxxxxxxxxxxxxxxxxx -Dtwitter4j.oauth.accessToken=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx -Dtwitter4j.oauth.accessTokenSecret=xxxxxxxxxxxxxxxxxxxxxxxxxx
