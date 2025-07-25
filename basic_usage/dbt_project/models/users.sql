select
    id as user_id,
    email,
    first_name,
    last_name,
    created_at,
    is_active,
    total_orders,
    case 
        when total_orders > 10 then 'premium'
        else 'standard'
    end as user_tier
from {{ ref('raw_users') }}