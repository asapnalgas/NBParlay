# Strict Milestone Plan v1

This plan enforces weekly go/no-go gates in strict order for competitive parity work.

## Source of truth

- Plan config: [strict_milestones_v1.json](/Users/josephdelallera/Documents/Playground%202/nba_model/config/strict_milestones_v1.json)
- Runner: [run_strict_milestones.py](/Users/josephdelallera/Documents/Playground%202/nba_model/scripts/run_strict_milestones.py)
- Latest report: [strict_milestone_status.json](/Users/josephdelallera/Documents/Playground%202/nba_model/models/strict_milestone_status.json)

## Weekly sequence

1. Week 1: Data Foundation
2. Week 2: Pipeline Hardening
3. Week 3: Runtime Performance
4. Week 4: Live Reliability
5. Week 5: Benchmark Data Sufficiency
6. Week 6: Benchmark Competitiveness
7. Week 7: Calibration and Error Reduction
8. Week 8: Coverage Depth
9. Week 9: Production Readiness
10. Week 10: Go-Live Gate

If strict ordering is enabled (default), the runner blocks evaluation of later weeks after the first failed week.

## Run commands

Evaluate all weeks:

```bash
cd "/Users/josephdelallera/Documents/Playground 2/nba_model"
"/Users/josephdelallera/Documents/Playground 2/venv/bin/python" scripts/run_strict_milestones.py
```

Evaluate through a specific week:

```bash
cd "/Users/josephdelallera/Documents/Playground 2/nba_model"
"/Users/josephdelallera/Documents/Playground 2/venv/bin/python" scripts/run_strict_milestones.py --through-week 4
```

Output file:

- [strict_milestone_status.json](/Users/josephdelallera/Documents/Playground%202/nba_model/models/strict_milestone_status.json)
