package vttp2022.paf.delanday28bgggame.Repository;

import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

import vttp2022.paf.delanday28bgggame.Model.Comment;
import vttp2022.paf.delanday28bgggame.Model.Game;

@Repository
public class BGGRepository {
    
    // #1. Autowired MongoTemplate
    @Autowired
    private MongoTemplate mongoTemplate;

    public Optional<Game> search(String name) {

        //Typical Pipeline Flow                 M - P - G - S - O 
        //                          Match -> Project -> Group -> Sort -> Out

        // •Create Stages

        // (MATCH)
        // 1. $match the name - filter documents 
        MatchOperation matchName = Aggregation.match(
            Criteria.where("name").regex(name,"i") //regex option i is case insensi, if not is sensi.
        );

        // 2. $lookup - Join 2 Collections (gid from Comments (have multiple -> array of Docs/Obj) join/match w gid from Games)
        // One Game doc will have comment field consisting an array of Comment Docs
        /*
         * $lookup: {
            from: ‹collection to join>,
            localField: ‹field from the input documents>
            foreignField: ‹field from the documents of the "from" collection>,
            as: ‹output array field>
            }
         */
        LookupOperation findComments = Aggregation.lookup("comments", "gid", "gid", "comments");

        // (PROJECT)
        // 3. $project: _id, name - Reshape documents, new structure
        ProjectionOperation selectFields = Aggregation.project("_id", "name", "image", "comments");

        // 4. $unwind - Seen in Step 2, comments field is an array -> need to unwind, if not cannot progress in next 
        // Can have SubDocs, but just cannot have Arrays of SubDocs etc
        // Restructure in step 3 then separate is more systematic
        UnwindOperation unwindComments = Aggregation.unwind("comments");

        // (SORT)
        // 5. $sort - Orders the documents
        SortOperation sortByRating = Aggregation.sort(Direction.DESC, "comments.rating");

        // $limit - Pagination paginate documents
        LimitOperation takeTop10 = Aggregation.limit(10);

        // (GROUP)
        // 6. $group - Summarizes documents
        GroupOperation groupByName = Aggregation.group("name", "image")
            .push("comments").as("comments");

        // Create the pipeline - include all the agg methods above
        Aggregation pipeline = Aggregation.newAggregation(
            matchName, findComments, selectFields, unwindComments, sortByRating, takeTop10, groupByName);

        //Query the collection
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "games", Document.class);

        // Check if need to return empty bc return is Optional
        if (!results.iterator().hasNext()) //if true means have more elements -> false -> then can extract
                                           //if false means have no elements -> true -> then return empty
            return Optional.empty();

        // If Not,
        // Get One result only,
        Document doc = results.iterator().next(); // returns next element in the iteration
        Game g = Game.create(doc);
        List<Comment> comments = doc.getList("comments", Document.class).stream()
            .map(c -> Comment.create(c))
            .toList();
        g.setComments(comments);

        return Optional.of(g);


    }

}
