● Here's what the research found — one key number changes the calculus on all four
  options:

  ρ = -0.0179 (essentially zero)

  The copula params from the trained artifact show IP and K/IP are uncorrelated. This
  isn't a surprise statistically — dividing K by IP to get K/IP inherently suppresses the
  IP variance, and the remaining K/IP variance is driven by pitcher stuff/command, not
  workload. This means the copula adds zero value over independent sampling regardless of
  how good the IP model becomes.

  With that context, here's the honest assessment of each option:

  ---
  Option 1 — Discrete hazard model

  Correctly models the pull decision as a survival process, which is the right statistical
   framing. But the output is still just a better IP distribution. Since ρ ≈ 0, knowing
  the precise IP distribution doesn't help predict K unless you use it as a feature
  (Option 4 logic). High engineering cost for a sub-model that doesn't directly connect to
   K prediction. Best use case: if IP itself is a betting market (it's not on Kalshi
  currently).

  Option 2 — Better features for existing IP quantile regression

  The three highest-impact missing features:
  - pitcher_min_ip_l5 — trivially easy (MIN instead of AVG, already have the inning data)
  - bullpen_ip_last_3d — mlb_bullpen_daily_status exists, but no bullpen features are
  wired yet
  - opp_team_pitches_per_pa_l10 — plate discipline data may need Statcast pull; not in
  current tables

  Better IP features alone don't fix the ρ ≈ 0 problem. The improvement stays trapped in
  the IP sub-model and doesn't flow through to K.

  Option 3 — BF × K/BF

  pitcher_est_bf_l5 already exists (3×IP + H + BB). But BF is ~0.98 correlated with IP, so
   ρ(BF, K/BF) will be near-zero for the same reason. Slightly narrower variance doesn't
  fix the structural independence. Not worth pursuing.

  Option 4 — IP model as feature source (your recommendation)

  This is the only option that sidesteps the ρ ≈ 0 problem. You stop trying to multiply IP
   × rate and instead ask: "does predicted short-outing risk improve the direct K model's
  calibration at the lower quantiles?" The answer is probably yes — the direct K model
  currently has no signal for "pitcher gets pulled in the 4th" scenarios. Adding
  predicted_ip_q25 and predicted_ip_spread as features to the K model gives it a
  risk-adjustment channel without requiring multiplicative error propagation.

  ---
  Verdict: Option 4 is the right path. The other three options don't resolve the core ρ ≈
  0 issue.

  Concrete work for Option 4:

  1. Add pitcher_min_ip_l5 to IP model features — 15 minutes (MIN aggregate, same SQL
  pattern)
  2. Wire bullpen_ip_last_3d from mlb_bullpen_daily_status — exists in the table, just not
   in the feature store yet
  3. Retrain the IP sub-model with better features
  4. Add predicted_ip_q25 and predicted_ip_spread as input features to the main K model's
  feature list
  5. Retrain the K model — it now has an "early-exit risk" signal baked in

  The architecture becomes: IP model → {q25, spread} → K model features, not IP × K/IP →
  K. Want to plan this out?

✻ Churned for 3m 17s

❯ The analysis is mostly right and the conclusion is correct, but there's one piece   
  of reasoning I want to push back on because it matters for how you think about this 
   going forward.                                                                     
  The explanation for why ρ ≈ 0 is partially wrong, and the error matters.            
  The analysis says: "dividing K by IP to get K/IP inherently suppresses the IP       
  variance, and the remaining K/IP variance is driven by pitcher stuff/command, not   
  workload."                                                                          
  That's a spurious correlation argument — the idea that because K/IP has IP in the   
  denominator, any correlation with IP is mechanically suppressed. This is a real     
  statistical phenomenon, but it's not the primary explanation here. If there were a  
  genuine positive biological/mechanical relationship between IP and K-rate (pitchers 
   who are dealing go deeper AND strike out more per inning), it would show up in the 
   data despite the ratio structure. The ratio suppression effect would dampen a      
  positive correlation, but it wouldn't eliminate a real one — it would just          
  attenuate it.                                                                       
  The actual reason ρ ≈ 0 is that, conditional on the features both models see, the   
  residual game-to-game variation in IP and K-rate genuinely comes from different     
  causal pathways. IP residuals are driven by pitch count accumulation (walks, foul   
  balls, long ABs), manager decisions, bullpen state, and game script. K-rate         
  residuals are driven by stuff quality that day, batter swing decisions, umpire      
  zone, and sequencing luck. These processes happen to be approximately independent   
  after conditioning on pitcher identity and matchup features, which is what the      
  training data reflects.                                                             
  Why does this matter practically? Because the analysis as written implies that ρ ≈  
  0 is somewhat mechanical or inevitable ("inherently suppresses"). If that were      
  true, you'd never expect to find signal by using IP predictions as features in the  
  K model either — the independence would be fundamental. But the real explanation —  
  that the causal pathways are different — actually supports Option 4. The IP model   
  captures information about the pitch-count-accumulation pathway that the K model    
  doesn't have access to. The K model can use that information even though the two    
  outcomes don't co-vary, because knowing "this pitcher is at risk of a short outing  
  tonight" changes the expected total K count even if it doesn't change the K-rate.   
  Think of it this way: if I told you a pitcher will throw exactly 4 innings tonight, 
   your K prediction drops. If I told you he'll throw exactly 8 innings, it goes up.  
  The IP prediction carries information about total K even though IP and K/IP are     
  uncorrelated. The correlation between IP and total K is substantial — it's the      
  correlation between IP and K-rate that's zero. Option 4 works precisely because     
  you're feeding IP information into a total-K model, not a K-rate model.             
  The rest of the analysis is clean. Option 3 is correctly dismissed — BF ≈ IP means  
  you're just relabeling the same axis. Options 1 and 2 are correctly identified as   
  producing improvements that stay trapped in the IP sub-model without a pathway to K 
   prediction. Option 4 is the right architecture.                                    
  One addition to the concrete work plan:                                             
  Between steps 3 and 4, add a validation step: check the correlation between         
  predicted_ip_q25 and the K model's existing features. If predicted_ip_q25 is 0.85+  
  correlated with pitcher_avg_ip_l5 (which is already in the K model's feature list), 
   the new feature won't add much — the K model already has a decent IP signal and is 
   choosing not to use it heavily. In that case, the issue isn't missing information, 
   it's that the model doesn't weight IP risk appropriately, and you might need to    
  look at why (possibly because pitcher_avg_ip_l5 is a mean that doesn't capture tail 
   risk, whereas predicted_ip_q25 does).                                              
  If the correlation is moderate — say 0.5-0.7 — that's the sweet spot. It means      
  predicted_ip_q25 carries genuinely new information about game-specific short-outing 
   risk that the static L5 average doesn't capture. That's where you'd expect the     
  biggest calibration improvement in the tails.                                       

● Correction accepted — that's a meaningful distinction and you're right that it matters.

  The ratio suppression argument is wrong as the primary explanation. The correct framing:
   IP and K/IP are empirically independent because their residual variance comes from
  genuinely different causal mechanisms after conditioning on features. That's a
  substantive fact about baseball, not a statistical artifact.

  And the implication you're drawing is correct. ρ(IP, K/IP) ≈ 0 tells you nothing about
  ρ(IP, K_total), which is probably 0.4–0.6. A pitcher going 4 innings versus 8 innings is
   the dominant driver of total K variance — the rate per inning is a secondary effect.
  Option 4 works precisely because you're routing IP information into a total-K model
  through the volume channel, not claiming K-rate and IP co-vary.

  The updated plan with the validation step:

  1. Add pitcher_min_ip_l5 to IP model features (MIN aggregate, trivial)
  2. Wire bullpen_ip_last_3d from mlb_bullpen_daily_status
  3. Retrain IP sub-model
  4. Validation: compute corr(predicted_ip_q25, pitcher_avg_ip_l5) on the training set
    - > 0.85: K model already has the IP signal via the L5 mean; the issue is mean vs.
  tail-risk framing — consider adding predicted_ip_q25 - pitcher_avg_ip_l5 as a delta
  feature instead of q25 raw
    - 0.5–0.7: Sweet spot — predicted_ip_q25 is capturing game-specific short-outing risk
  beyond the static average; add it directly
    - < 0.5: IP model is doing something idiosyncratic; investigate before wiring it in
  5. Add predicted_ip_q25 and predicted_ip_spread (or delta variant) to K model features
  6. Retrain K model

  Want to start on the implementation plan for this?

❯ also, you threw out the idea that the bullpen features werent wired in yet. Thats   
  not true. Here is the evidence:                                                     
   Good news — both are wired in. The claim was wrong. Here's the actual state:       
                                                                                      
    Pitcher inning stats (mlb_pitcher_inning_stats) — WIRED IN                        
    - 7 features in PITCHER_K_FEATURES (velo drop, whiff rate, K rate                 
    early/late, pitches/inning, deep inning pct, K first 5ip)                         
    - 3 opposing pitcher features in BATTER_BASE_FEATURES (velo drop,                 
    pitches/inning, deep inning pct)                                                  
    - Both training queries and single-game inference pull from this table            
                                                                                      
    Bullpen features (mlb_bullpen_daily_status) — WIRED IN                            
    - 4 features in BATTER_BASE_FEATURES: opp_bullpen_ip_last_3d,                     
    opp_bullpen_era_last_7d, opp_relievers_available,                                 
    opp_bullpen_pitches_last_3d                                                       
    - Used in batter models (hits, HRR, etc.) — not pitcher strikeout models          
                                                                                      
    The catch: both are behind COALESCE with sensible defaults (0 IP, 4.50 ERA,       
    5 relievers, etc.), so if the tables are empty or stale, the model silently       
    uses fallback values and won't error. The features are wired in code but          
    only as good as the data being scraped.                                           
                                                                                      
    Do you want me to check whether mlb_pitcher_inning_stats and                      
    mlb_bullpen_daily_status are actually populated with recent data?                 

● Good correction. Let me update the picture:

  Actual state of features:
  - Inning stats → already in PITCHER_K_FEATURES (7 features wired Apr 28)
  - Bullpen features → in BATTER_BASE_FEATURES only, not in pitcher K feature set
  - The bullpen data exists and is scraped — just not routed to the pitcher side

  So the actual gaps for the IP sub-model are narrower than I thought:

  1. pitcher_min_ip_l5 — doesn't exist, only the mean (pitcher_avg_ip_l5)
  2. Bullpen workload — the data is there (mlb_bullpen_daily_status), computed for batters
   as opp_bullpen_ip_last_3d, just needs to be added to the pitcher training query from
  the pitcher's team perspective (rested pen → shorter leash, tired pen → longer leash)
  3. opp_team_pitches_per_pa_l10 — doesn't exist anywhere yet; might not be worth the
  Statcast pull if the other two cover most of the variance

  The revised concrete work:
  1. Add pitcher_min_ip_l5 to pitcher feature store (MIN instead of AVG, same inning-stats
   SQL pattern — trivial)
  2. Add team_bullpen_ip_last_3d (pitcher's own team bullpen workload) to IP sub-model
  features — SQL already exists for batter side, just needs the join flipped
  3. Retrain IP sub-model
  4. Validate corr(predicted_ip_q25, pitcher_avg_ip_l5) — determines whether to add raw
  q25 or the delta
  5. Add IP quantile features to K model, retrain

  Want to plan the implementation?

✻ Worked for 30s

❯ /plan 
  ⎿  Enabled plan mode

❯ yes make a plan for implementation 

● 2 Explore agents finished (ctrl+o to expand)
   ├─ Explore pitcher feature store SQL and IP training · 18 tool uses · 62.5k tokens
   │  ⎿  Done
   └─ Explore bullpen SQL join in batter feature store · 3 tool uses · 31.7k tokens
      ⎿  Done

● Explore(Resume pitcher feature store exploration) resuming aebcfcf
  ⎿  Done (0 tool uses · 65.0k tokens · 30s)
  (ctrl+o to expand)

● Good, I have everything I need. Let me write the implementation plan.

● Updated plan
  ⎿  /plan to preview
  ⎿  User rejected Claude's plan:
     ╭───────────────────────────────────────────────────────────────────────────────────╮
     │ Plan: IP Model as Feature Source for Pitcher K                                    │
     │                                                                                   │
     │ Context                                                                           │
     │                                                                                   │
     │ Copula backtest (2026-04-13 to 2026-04-26) showed the copula is worse than the    │
     │ single model.                                                                     │
     │ Root cause: ρ(IP, K/IP) = -0.018 (near zero) — not because of ratio suppression,  │
     │ but because                                                                       │
     │ IP and K/IP have genuinely different causal pathways (pitch-count accumulation    │
     │ vs. stuff quality).                                                               │
     │ The copula architecture is sound, but the IP sub-model over-predicts depth        │
     │ because it's missing                                                              │
     │ short-outing features. Option 4: improve IP model and feed IP quantile            │
     │ predictions as features                                                           │
     │ into the direct K model (volume signal pathway). The copula remains as an         │
     │ architecture but the                                                              │
     │ direct K model gains IP uncertainty as a risk-adjustment feature.                 │
     │                                                                                   │
     │ Architecture After This Change                                                    │
     │                                                                                   │
     │ 1. IP sub-model trains on pitcher features → produces IP quantile predictions     │
     │ 2. At K model training time: generate batch IP predictions on training data,      │
     │ append                                                                            │
     │ predicted_ip_q25 and predicted_ip_spread as new columns                           │
     │ 3. Direct K model trains on enriched features (including IP quantile columns)     │
     │ 4. At inference: IP model runs first → q25/spread appended to features dict → K   │
     │ model runs                                                                        │
     │                                                                                   │
     │ Critical Files                                                                    │
     │                                                                                   │
     │ - src/models/mlb/mlb_feature_store.py — PITCHER_K_FEATURES list + SQL queries     │
     │ - src/processing/mlb/mlb_populate_averages.py (or equivalent — find script that   │
     │ populates                                                                         │
     │ mlb_player_average_pitching table, uses rolling_with_groupby)                     │
     │ - src/models/mlb/mlb_train_pipeline.py — copula + single-model training           │
     │ orchestration                                                                     │
     │ - src/models/mlb/mlb_monte_carlo.py — predictor classes                           │
     │ - src/models/mlb/mlb_model_suite.py — loading logic                               │
     │                                                                                   │
     │ Step-by-Step Implementation                                                       │
     │                                                                                   │
     │ Phase 1: Static Feature Additions                                                 │
     │                                                                                   │
     │ Step 1 — Add pitcher_min_ip_l5 to populate averages script:                       │
     │ - Find the script that populates mlb_player_average_pitching (likely              │
     │ src/processing/mlb/)                                                              │
     │ - It already uses rolling_with_groupby(ip_shifted, group_key, window=5,           │
     │ agg="sum")                                                                        │
     │ - Add: min_ip_l5 = rolling_with_groupby(ip_shifted, group_key, window=5,          │
     │ agg="min")                                                                        │
     │ - Write result to the DataFrame alongside avg_ip_l5                               │
     │                                                                                   │
     │ Step 2 — DB migration (local only):                                               │
     │ ALTER TABLE mlb_player_average_pitching ADD COLUMN IF NOT EXISTS min_ip_l5 FLOAT; │
     │ UPDATE mlb_player_average_pitching SET min_ip_l5 = 0 WHERE min_ip_l5 IS NULL;     │
     │ Then re-run the populate script to fill values.                                   │
     │                                                                                   │
     │ Step 3 — Wire pitcher_min_ip_l5 into feature store:                               │
     │                                                                                   │
     │ In mlb_feature_store.py, in the LATERAL JOIN against mlb_player_average_pitching: │
     │ -- Add to SELECT:                                                                 │
     │ pa.min_ip_l5 AS pitcher_min_ip_l5                                                 │
     │ Add "pitcher_min_ip_l5" to PITCHER_K_FEATURES list.                               │
     │ Add to get_player_game_features() return dict.                                    │
     │                                                                                   │
     │ Step 4 — Add team_bullpen_ip_last_3d to pitcher feature store:                    │
     │                                                                                   │
     │ In mlb_feature_store.py training query, add a LEFT JOIN (mirror batter side but   │
     │ own team):                                                                        │
     │ LEFT JOIN mlb_bullpen_daily_status bull_own                                       │
     │     ON bull_own.team_id = pgs.team_id                                             │
     │    AND bull_own.game_date = pgs.game_date                                         │
     │ Add to SELECT:                                                                    │
     │ COALESCE(bull_own.bullpen_ip_last_3d, 0) AS team_bullpen_ip_last_3d,              │
     │ COALESCE(bull_own.bullpen_pitches_last_3d, 0) AS team_bullpen_pitches_last_3d     │
     │ Add both to PITCHER_K_FEATURES. Add to get_player_game_features().                │
     │                                                                                   │
     │ Phase 2: Retrain IP Sub-Model                                                     │
     │                                                                                   │
     │ Step 5: Run populate averages script to fill min_ip_l5.                           │
     │                                                                                   │
     │ Step 6: Retrain copula (IP sub-model + K-rate sub-model):                         │
     │ python src/models/mlb/mlb_train_pipeline.py --local --copula \                    │
     │   --tune --tuning-trials 100 \                                                    │
     │   --train-seasons 2024 2025 \                                                     │
     │   --cal-season 2025 --cal-end-date 2025-07-01                                     │
     │ New IP sub-model will have pitcher_min_ip_l5 and team_bullpen_ip_last_3d          │
     │ available for                                                                     │
     │ dynamic feature selection.                                                        │
     │                                                                                   │
     │ Phase 3: Validation Gate                                                          │
     │                                                                                   │
     │ Step 7 — Correlation check:                                                       │
     │ After retraining, generate IP predictions on 2025-07-01 → 2025-09-28 holdout      │
     │ data.                                                                             │
     │ Compute: corr(predicted_ip_q25, pitcher_avg_ip_l5)                                │
     │                                                                                   │
     │ - > 0.85: IP model isn't adding much beyond static L5 mean. Use predicted_ip_q25  │
     │ - pitcher_avg_ip_l5 as the delta feature instead of raw q25 (captures             │
     │ game-specific deviation                                                           │
     │ from baseline). May mean pitcher_min_ip_l5 alone is sufficient.                   │
     │ - 0.5–0.7: Sweet spot — IP model captures game-specific short-outing risk. Add    │
     │ raw q25                                                                           │
     │ and spread as K model features.                                                   │
     │ - < 0.5: IP model is noisy — investigate before wiring in.                        │
     │                                                                                   │
     │ Phase 4: Wire IP Predictions into K Model Training                                │
     │                                                                                   │
     │ Step 8 — Modify mlb_train_pipeline.py (non-copula path):                          │
     │                                                                                   │
     │ After IP sub-model training (done as part of copula run or standalone), generate  │
     │ batch IP                                                                          │
     │ predictions on the K model training DataFrame:                                    │
     │ # After ip_pipeline is trained:                                                   │
     │ X_ip = train_df[ip_pipeline.feature_names]  # use IP model's selected features    │
     │ ip_preds = ip_pipeline.predict_batch(X_ip)  # returns DataFrame with q25/q75      │
     │ columns                                                                           │
     │ train_df["predicted_ip_q25"] = ip_preds["q0.25"]                                  │
     │ train_df["predicted_ip_spread"] = ip_preds["q0.75"] - ip_preds["q0.25"]           │
     │ These columns are now available to the K model's dynamic feature selector.        │
     │                                                                                   │
     │ Step 9 — Modify inference: mlb_monte_carlo.py                                     │
     │                                                                                   │
     │ Add/modify MLBMonteCarloPredictor (or create MLBPitcherKWithIPPredictor):         │
     │ def predict(self, features: dict) -> PropPrediction:                              │
     │     if self.ip_pipeline is not None:                                              │
     │         # Generate IP quantiles first                                             │
     │         ip_pred = self.ip_pipeline.predict_single(features)                       │
     │         features["predicted_ip_q25"] = ip_pred.q25                                │
     │         features["predicted_ip_spread"] = ip_pred.q75 - ip_pred.q25               │
     │     # Then run K model as normal                                                  │
     │     ...                                                                           │
     │                                                                                   │
     │ Step 10 — mlb_model_suite.py loading:                                             │
     │ When loading the K model, also load the IP pipeline (from ip_model/ subdir if     │
     │ present).                                                                         │
     │ Pass it to the K predictor so inference uses the two-stage path.                  │
     │                                                                                   │
     │ Phase 5: Retrain K Model and Backtest                                             │
     │                                                                                   │
     │ Step 11: Retrain direct K model (single model path, with IP features):            │
     │ python src/models/mlb/mlb_train_pipeline.py --local \                             │
     │   --tune --tuning-trials 100 \                                                    │
     │   --train-seasons 2024 2025 \                                                     │
     │   --cal-season 2025 --cal-end-date 2025-07-01                                     │
     │                                                                                   │
     │ Step 12: Backtest sweep, same params as the original comparison:                  │
     │ python src/backtesting/mlb/run_mlb_sweep.py --local \                             │
     │   --stats pitcher_strikeouts \                                                    │
     │   --model-dir src/models/mlb/artifacts/<new_run_dir> \                            │
     │   --start 2026-03-27 --end 2026-04-29 \                                           │
     │   --tau none 0.25 0.5 0.75 0.9 \                                                  │
     │   --edge 0.08 0.10 0.12 0.15 \                                                    │
     │   --z-max 0.25 1.0 --max-weight 0.5 0.8 \                                         │
     │   --n-samples 5000                                                                │
     │ Compare no-BL baseline ROI vs. old single model's no-BL baseline (was ~+2.88% at  │
     │ edge=0.10).                                                                       │
     │ If new no-BL baseline is positive and > old, the IP feature is adding real        │
     │ signal.                                                                           │
     │                                                                                   │
     │ Verification                                                                      │
     │                                                                                   │
     │ - IP model's predicted_ip_q25 on holdout: aces should show q25 ~5.5–6.0 IP;       │
     │ soft-tossers ~4.5–5.0                                                             │
     │ - team_bullpen_ip_last_3d SHAP importance should show up in top-20 for IP model   │
     │ - pitcher_min_ip_l5 SHAP importance should show up in top-15 for IP model         │
     │ (captures blowup risk)                                                            │
     │ - K model no-BL backtest baseline should improve over current (-3.56% at          │
     │ edge=0.15 → closer to 0 or positive)                                              │
     │ - The lower quantiles (K model Q25 predictions) should be better calibrated on    │
     │ short-outing games                                                                │
     ╰───────────────────────────────────────────────────────────────────────────────────╯

❯ write all of that to a .md file for the braintree 

● Write(brain\Decisions\Pitcher-K-IP-Feature-Source-May01.md)
  ⎿  Wrote 1 lines to brain\Decisions\Pitcher-K-IP-Feature-Source-May01.md
     # Pitcher K: IP Model as Feature Source Architecture

     **Date**: May 1, 2026                                                                
     **Status**: Planned — not yet implemented
     **Precedes**: [[MLB-Model-Architecture-Overhaul-Apr28]]                              
                                                            
     ## Why This Change

     Copula backtest (Apr 13–26, 2026) showed the copula model is worse than the
     single model.
     The no-BL baseline for the copula was entirely negative across all edge
     thresholds, while the

● Written to brain/Decisions/Pitcher-K-IP-Feature-Source-May01.md. It covers the full
  decision rationale (ρ ≈ 0 causal explanation, why Option 4 works through the volume
  channel), the two missing features, the validation gate with the correlation threshold  
  table, and all 5 implementation phases with exact code snippets.

  