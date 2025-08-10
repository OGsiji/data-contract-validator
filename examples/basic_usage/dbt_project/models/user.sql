-- Simple users model
select
    id as user_id,
    email,
    first_name,
    last_name,
    created_at,
    is_active
from raw_users
