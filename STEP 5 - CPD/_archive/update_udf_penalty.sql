-- Update UDF penalty parameter to optimal value from TICKET-124
-- Cannot modify UDF directly due to permissions, but this documents the required change

-- CURRENT UDF uses: pen=0.1
-- OPTIMAL VALUE from TICKET-124 two-phase validation: pen=0.05
-- ACHIEVED RECALL: 100% (exceeds 95% target)

-- The following change needs to be applied by a superuser:
-- Line 34: change_points = algo.predict(pen=0.1)
-- Should become: change_points = algo.predict(pen=0.05)

SELECT 'UDF update required: Change penalty parameter from 0.1 to 0.05 for optimal performance' AS status;