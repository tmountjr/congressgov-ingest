"""Generate views on the database"""

from sqlalchemy import text
from sqlalchemy.orm import Session
from database.base import BaseOrm


def create_views(data_dir):
    """Create views in the database."""

    orm = BaseOrm(data_dir)
    engine = orm.engine
    with Session(engine) as session:
        # Create a view that shows only the last vote on each vote ID.
        session.execute(text("DROP VIEW IF EXISTS latest_vote_ids"))
        session.execute(
            text(
                """
CREATE VIEW latest_vote_ids as
WITH
  temp_vm AS (
    SELECT
      vm_1.vote_number,
      vm_1.vote_id,
      vm_1.bill_id,
      vm_1.chamber,
      vm_1.date,
      vm_1.result,
      vm_1.category,
      vm_1.nomination_title,
      vm_1.source_filename,
      CASE
        WHEN (vm_1.bill_id IS NULL) THEN (
          SUBSTRING(
            vm_1.nomination_title
            FROM
              1 FOR 10
          )
        )::character varying
        ELSE vm_1.bill_id
      END AS unique_matching_field
    FROM
      vote_meta vm_1
  )
SELECT
  vm.vote_number,
  vm.vote_id,
  vm.bill_id,
  vm.chamber,
  vm.date,
  vm.result,
  vm.category,
  vm.nomination_title,
  vm.source_filename,
  vm.unique_matching_field
FROM
  (
    temp_vm vm
    JOIN (
      SELECT
        temp_vm.unique_matching_field,
        max(temp_vm.date) AS latest_date
      FROM
        temp_vm
      GROUP BY
        temp_vm.unique_matching_field
    ) latest_votes ON (
      (
        (
          (vm.unique_matching_field)::text = (latest_votes.unique_matching_field)::text
        )
        AND (vm.date = latest_votes.latest_date)
      )
    )
  );"""
            )
        )

        # Create a view that adds sponsor name and party to the vote_meta table.
        session.execute(text("DROP VIEW IF EXISTS enriched_vote_meta"))
        session.execute(
            text(
                """
CREATE VIEW enriched_vote_meta as
SELECT
  vote_meta.vote_number,
  vote_meta.vote_id,
  vote_meta.bill_id,
  vote_meta.chamber,
  vote_meta.date,
  vote_meta.result,
  vote_meta.category,
  vote_meta.nomination_title,
  vote_meta.amendment_id,
  vote_meta.source_filename,
  CASE
    WHEN (vote_meta.amendment_id IS NOT NULL) THEN a_sponsor.name
    ELSE b_sponsor.name
  END AS sponsor_name,
  CASE
    WHEN (
      (vote_meta.category)::text = ANY (
        (
          ARRAY[
            'nomination'::character varying,
            'leadership'::character varying,
            'quorum'::character varying,
            'procedural'::character varying
          ]
        )::text[]
      )
    ) THEN 'R'::character varying
    WHEN (
      ((vote_meta.category)::text = 'cloture'::text)
      AND (vote_meta.nomination_title IS NOT NULL)
    ) THEN 'R'::character varying
    WHEN (
      (vote_meta.amendment_id IS NOT NULL)
      AND (amendments.sponsor_id IS NOT NULL)
    ) THEN a_sponsor.party
    WHEN (
      (vote_meta.amendment_id IS NOT NULL)
      AND (amendments.sponsor_id IS NULL)
    ) THEN 'R'::character varying
    ELSE b_sponsor.party
  END AS sponsor_party
FROM
  (
    (
      (
        (
          vote_meta
          LEFT JOIN bills ON (
            ((vote_meta.bill_id)::text = (bills.bill_id)::text)
          )
        )
        LEFT JOIN amendments ON (
          (
            (vote_meta.amendment_id)::text = (amendments.amendment_id)::text
          )
        )
      )
      LEFT JOIN legislators a_sponsor ON (
        (
          (amendments.sponsor_id)::text = (a_sponsor.bioguide_id)::text
        )
      )
    )
    LEFT JOIN legislators b_sponsor ON (
      (
        (bills.sponsor_id)::text = (b_sponsor.bioguide_id)::text
      )
    )
  )
ORDER BY
  vote_meta.chamber,
  vote_meta.vote_number;"""
            )
        )

        session.commit()
