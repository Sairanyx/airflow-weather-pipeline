                     ┌────────────────────────┐
                     │        EXTRACT         │
                     │------------------------│
                     │ Download ZIP from      │
                     │ Kaggle API             │
                     │ Unzip → weatherHistory │
                     │ XCom: extracted_path   │
                     └───────────┬────────────┘
                                 │
                                 ▼
                     ┌────────────────────────┐
                     │       TRANSFORM        │
                     │------------------------│
                     │ Cleaning steps         │
                     │ Feature Engineering    │
                     │ Create daily CSV       │
                     │ Create monthly CSV     │
                     │ XCom: daily, monthly   │
                     └──────┬────────┬────────┘
                            │        │
                ┌───────────▼───┐  ┌─▼────────────────┐
                │ VALIDATE DAILY│  │VALIDATE MONTHLY   │
                │---------------│  │--------------------│
                │ Check columns │  │ Check columns      │
                │ Missing data  │  │ Missing data       │
                │ Ranges        │  │ Ranges             │
                │ XCom: valid   │  │ XCom: valid        │
                └───────┬───────┘  └─────────┬─────────┘
                        │                    │
                        └────────┬────────────┘
                                 ▼
                     ┌────────────────────────┐
                     │          LOAD          │
                     │------------------------│
                     │ Insert into DB:        │
                     │ - daily_weather table  │
                     │ - monthly_weather table│
                     └────────────────────────┘
