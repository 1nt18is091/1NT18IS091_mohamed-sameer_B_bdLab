// Demonstrate the usage of $match.
db.movies.aggregate([{$match:{"Referable":"TRUE","Genre":"Romance"}}]).pretty()

// Demonstrate the usage of $group.
db.movies.aggregate([{$group: { _id: "$Referable", referable_count: {$sum:1}} } ])

// Demonstrate the usage of aggregate pipelines.
db.movies.aggregate([{$match :{"Referable":"TRUE"}},{$group :{_id :"$Genre",referable_count : {$sum:"$IMDB Rating"} }}])

// Demonstrate the Map-Reduce aggregate function on this dataset.
var mapF=function(){emit(this["Genre"],this["IMDB Rating"]);};
var redF=function(mname,mrating){return Array.avg(mrating);};
db.movies.mapReduce(mapF,redF,{out:"mapreduceout"})
db.mapreduceout.find().pretty();

// Count the number of Movies which belong to the Romance/Thriller category and find out the total number of positive reviews in that category.
db.movies.aggregate([{$match: { Genre: "Romance"} }, {$group: { _id: "$Genre", feedback_count:{$sum:"$Positive Feedback"}, thriller_count: {$sum:1}}} ]).pretty()
db.movies.aggregate([{$match: { Genre: "Thriller"} }, {$group: { _id: "$Genre", feedback_count:{$sum:"$Positive Feedback"}, thriller_count: {$sum:1}}} ])

// Group all the records by genre and find out the total number of positive feedbacks by genre.
db.movies.aggregate([{$group: { _id: "$Genre", feedback_sum: {$sum:"$Positive Feedback"},genre_count: {$sum:1}} } ])
