-- User profiles model
select
    id as user_id,
    email,
    first_name || ' ' || last_name as full_name,
    bio
from raw_user_profiles
