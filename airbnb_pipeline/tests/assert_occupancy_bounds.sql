select *
from {{ ref('est_occupancy') }}
where est_occupancy < 0 or est_occupancy > 0.70