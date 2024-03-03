-- create and insert table shipping_country_rates

drop table if exists public.shipping_country_rates cascade;
create table public.shipping_country_rates(
	shipping_country_id 		serial not null,
	shipping_country 			text not null,
	shipping_country_base_rate 	numeric(14,2) null,
	primary key					(shipping_country_id)
);
create index shipping_country_id_index on public.shipping_country_rates(shipping_country_id);

insert into public.shipping_country_rates (shipping_country, shipping_country_base_rate)
select distinct
	shipping_country,
	shipping_country_base_rate
from public.shipping;

-- select * from public.shipping_country_rates limit 10




-- create and insert table shipping_agreement

drop table if exists public.shipping_agreement cascade;
create table public.shipping_agreement(
	agreementid 				int4 not null,
	agreement_number 			text not null,
	agreement_rate 				numeric(14,2) default 0.00,
	agreement_commission 		numeric(14,2) default 0.00,
	primary key 				(agreementid)
);
create index agreementid_index on public.shipping_agreement(agreementid);

insert into public.shipping_agreement
select distinct 
	"desc"[1]::int4,
	"desc"[2]::text,
	"desc"[3]::numeric(14,2),
	"desc"[4]::numeric(14,2)
from (select regexp_split_to_array(vendor_agreement_description, E':+') "desc" from public.shipping) a;

-- select * from public.shipping_agreement limit 10


-- create and insert table shipping_transfer

drop table if exists public.shipping_transfer cascade;
create table public.shipping_transfer(
	transfer_type_id 			serial not null,
	transfer_type 				char(2) not null,
	transfer_model 				text not null,
	shipping_transfer_rate 		numeric(14,3) default 0.000,
	primary key 				(transfer_type_id)
);
create index tranfer_type_id_index on public.shipping_transfer(transfer_type_id);

insert into public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
select distinct 
	(regexp_split_to_array(shipping_transfer_description, E'\:'))[1],
	(regexp_split_to_array(shipping_transfer_description, E'\:'))[2],
	shipping_transfer_rate
from public.shipping;

-- select * from public.shipping_transfer




-- create and insert table shipping_info

drop table if exists public.shipping_info cascade;
create table public.shipping_info(
	shippingid 					int8 not null,
	vendorid 					int2 not null,
	payment_amount 				double precision not null,
	shipping_plan_datetime 		timestamp not null,
	transfer_type_id 			int4 not null,
	shipping_country_id 		int4 not null,
	agreementid 				int4 not null,
	primary key 				(shippingid),
	foreign key 				(transfer_type_id) references public.shipping_transfer(transfer_type_id) on update cascade,
	foreign key 				(shipping_country_id) references public.shipping_country_rates(shipping_country_id) on update cascade,
	foreign key 				(agreementid) references public.shipping_agreement(agreementid) on update cascade
);
create index shippingid_index on public.shipping_info(shippingid);

insert into public.shipping_info
select
	s.shippingid,
	vendorid,
	payment,
	shipping_plan_datetime,
	st.transfer_type_id,
	sa.shipping_country_id,
	agreement_id
from 
	(select distinct
			shippingid,
			vendorid,
			payment,
			shipping_plan_datetime,
			shipping_country,
			(regexp_split_to_array(shipping_transfer_description, E'\:'))[1] as transfer_type,
			(regexp_split_to_array(shipping_transfer_description, E'\:'))[2] as transfer_model,
			(regexp_split_to_array(vendor_agreement_description, E'\:'))[1]::int4 as agreement_id
		from public.shipping) as s
inner join public.shipping_country_rates as sa 
	using (shipping_country) 
inner join public.shipping_transfer as st 
	on st.transfer_type::char(2) = s.transfer_type and st.transfer_model::text = s.transfer_model;

-- select * from public.shipping_info limit 10




-- create and insert table shipping_status

drop table if exists public.shipping_status cascade;
create table public.shipping_status(
	shippingid 						int8 not null,
	status							text not null,
	state							text not null,		
	shipping_start_fact_datetime	timestamp null,
	shipping_end_fact_datetime 		timestamp null,
	primary key 					(shippingid)
);
create index shippingid_status_index on public.shipping_status(shippingid);

insert into public.shipping_status
select 
	a.shippingid,
	a.status,
	a.state,
	max(case when state = 'booked' then state_datetime else null end) as shipping_start_fact_datetime,
	max(case when state = 'recieved' then state_datetime else null end) as shipping_end_fact_datetime
from 
	(select 
			shippingid,
			status,
			state,
			state_datetime,
			max(state_datetime) over (partition by shippingid) as max_dt_state
		from public.shipping
		group by 1,2,3,4) as a 
where  state_datetime = max_dt_state
group by 1,2,3;

-- select * from public.shipping_status




-- create view public.shipping_datamart

create or replace view public.shipping_datamart as 
select
	si.shippingid,
	si.vendorid,
	st.transfer_type,
	date_part('day', age(ss.shipping_end_fact_datetime, ss.shipping_start_fact_datetime)) as full_day_at_shipping,
	case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end as is_delay,
	case when ss.status = 'finished' then 1 else 0 end as is_shipping_finish,
	case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
		then date_part('day', age(ss.shipping_end_fact_datetime, si.shipping_plan_datetime)) else null end as delay_day_at_shipping,
	si.payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat,
	si.payment_amount * sa.agreement_commission as profit	
from public.shipping_status ss 
	inner join public.shipping_info si on ss.shippingid = si.shippingid 
	inner join public.shipping_transfer st on st.transfer_type_id = si.transfer_type_id 
	inner join public.shipping_country_rates scr on scr.shipping_country_id = si.shipping_country_id 
	inner join public.shipping_agreement sa on sa.agreementid = si.agreementid;

-- select * from public.shipping_datamart





