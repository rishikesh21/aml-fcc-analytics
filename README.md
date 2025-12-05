
I linked the three input tables (Models, Rules, Hits) into one weekly alert pipeline:
	•	Used Models to get the alert threshold per model and customer type (e.g., Mod1_INV → Mod1 + Individual).
	•	Used Rules to map each rule hit to a model + score for the right customer type.
	•	Processed Hits by parsing dates, assigning each hit to a week ending Sunday, and applying the dedup rule (same customer + same rule + same week counts once).
	•	Summed weekly scores per customer/model and compared against thresholds to decide if an alert is triggered.

From the final alerts, I generated the three required outputs (trends, rule overview, general stats).

How I found the data/config mistake (Mod3)

I added a simple sanity check: for each model/customer type, I computed the maximum possible score from the rules and compared it to the model threshold.

This flagged Mod3 (Individual): its threshold is 30, but the rules only add up to 20, so Mod3 can never trigger an alert with the current configuration.

Files
	•	Script: src/pipeline.py
	•	Outputs: data/output/ (alerts_trends_overview.csv, rule_hits_overview.csv, general_statistics.csv)
