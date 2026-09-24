Source data audit 
drived feature audit 
odds and temoral integrait audit 
automation audit 

Phase 2: repair critical paths 

Phase 3: Incumbent nba model audit 
    - articact idenity  
    - Functional validation 
    - calidbration and drift 
    - Better performance audit 


I think that for the old models, we should just use them as templates for the feature sets and training runs that we should be making. They are certain to be outdated - we are going into a new season. in fact, the season hasn't even started yet, we have no new data past what we have seen at the end of the season last year, and that end of season data needs to likely be excluded anyways (Playoffs, last x games where starters were benched. )

Phase 4: buld season restart training dataset 

Audit train/serve partiy - what does this mean? 
    - verify training and daily infernce use the same feature nam,es, ordres, etc
    - pRevents a model from being trained on one thing and then being used for another 

Nba contract featuers:
    - the fied input lists of features each model expects 

Freeze a dataset snapshot/ reproducible extraction manifest:
    - Prservce the exact training data, or record everything needed to recreate that training data

Phase 5: baseline retraining and evaluation
    - Reproduct incumbent config. 
    - Retrain each existing model against refreshed dataset - no need, no new data yet 

Phase 6: frontend 
