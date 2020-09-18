# -*- coding:utf-8 -*-
import os
import csv
import psycopg2
from datetime import datetime
from configparser import ConfigParser

CANDIDACY_FIELDS = [
    "e_candidacies.id",
    "e_candidacies.user_id",
    "e_candidacies.job_id",
    "e_candidacies.pipeline_stage_id",
    "e_candidacies.added_to_stage_at",
    "e_candidacies.score",
    "e_candidacies.possible_score",
    "e_candidacies.weighted_percentage_score",
    "e_candidacies.archive_reason",
    "e_candidacies.parent_candidacy_id",
    "e_candidacies.status",
    "e_candidacies.progress",
    "e_candidacies.failed",
    "e_candidacies.withdraw_reason",
    "e_candidacies.percentile",
    "e_candidacies.completed_user_assessments",
    "e_candidacies.scoring_completed",
    "e_candidacies.external_job_id",
    "e_candidacies.remaining_assessment_count",
    "e_candidacies.created_at",
    "e_candidacies.updated_at",
    "e_pipeline_stages.id",
    "e_pipeline_stages.name",
    "e_pipeline_stages.sequence",
    "e_pipeline_stages.slug",
    "e_pipeline_stages.job_id",
    "e_pipeline_stages.maintain_anonymity",
    "e_pipeline_stages.type",
    "e_pipeline_stages.created_at",
    "e_pipeline_stages.updated_at",
    "e_users.id",
    "e_users.email",
    "e_users.first_name",
    "e_users.middle_name",
    "e_users.last_name",
    "e_users.country_code",
    "e_users.phone",
    "e_users.created_at",
    "e_users.updated_at",
]


class JobCandidates:
    def __init__(self):
        config = ConfigParser()
        config.read('./config.ini')
        self.DBNAME = config["DATABASE"]["DBNAME"]
        self.USER = config["DATABASE"]["USER"]
        self.PASSWORD = config["DATABASE"]["PASSWORD"]
        self.HOST = config["DATABASE"]["HOST"]
        self.PORT = config["DATABASE"]["PORT"]

    def connect_psql(self, sql):
        conn = psycopg2.connect(database=self.DBNAME, user=self.USER, password=self.PASSWORD, host=self.HOST, port=self.PORT)
        # print("Opened database successfully")

        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        # print(rows)
        # print('fields:', [desc[0] for desc in cur.description])

        conn.commit()
        # print("Operation done successfully")
        cur.close()
        conn.close()

        return rows

    def get_candidacies(self, job_ids):

        conn = psycopg2.connect(database=self.DBNAME, user=self.USER, password=self.PASSWORD, host=self.HOST, port=self.PORT)

        query = """
            select
                string_agg(format(
                    '%%1$s.%%2$s as "%%1$s.%%2$s"',
                    attrelid::regclass, attname
                ) , ', ')
            from pg_attribute
            where attrelid = any (%s::regclass[]) and attnum > 0 and not attisdropped
        """

        cur = conn.cursor()
        cur.execute(query, ([table for table in ("e_candidacies", "e_users", "e_pipeline_stages")],))
        select_list = cur.fetchone()[0]

        select_candidacies_sql = """
                    SELECT {} 
                    FROM e_candidacies
                    INNER JOIN e_users ON e_candidacies.user_id=e_users.id 
                    INNER JOIN e_pipeline_stages 
                    ON e_pipeline_stages.job_id=e_candidacies.job_id AND e_candidacies.pipeline_stage_id=e_pipeline_stages.id
                    WHERE e_candidacies.job_id IN {} ORDER BY e_candidacies.created_at;
                """.format(
            select_list, job_ids
        )

        cur.execute(select_candidacies_sql)
        rows = cur.fetchall()
        # print(rows)
        # print("fields:", [desc[0] for desc in cur.description])

        conn.commit()
        cur.close()
        conn.close()

        return rows

    def process(self, job_ids_str, cur_path):
        print(job_ids_str, type(job_ids_str))
        job_ids = f"({job_ids_str})"
        print(job_ids, type(job_ids))
        print("==================process==================")

        scoring_dimension_ids = []

        select_organization_sql = f"SELECT DISTINCT organization_id FROM e_jobs WHERE id IN {job_ids};"
        organizations = self.connect_psql(select_organization_sql)
        print("organizations: ", organizations)

        org_id = organizations[0][0] if len(organizations) > 0 else "NULL"
        print("org_id: ", org_id)

        select_custom_field_sql = f"SELECT DISTINCT * FROM e_custom_fields WHERE organization_id={org_id};"
        custom_fields = self.connect_psql(select_custom_field_sql)
        print("custom_fields length: ", len(custom_fields))

        sql = """
            SELECT a.* 
            FROM e_assessments as a 
            INNER JOIN e_job_assessments as ja ON ja.assessment_id = a.id 
            WHERE ja.job_id IN {} order by ja.sequence;
        """.format(
            job_ids
        )
        ordered_but_potentially_duplicated_assessments = self.connect_psql(sql)
        print(
            "ordered_but_potentially_duplicated_assessments length: ",
            len(ordered_but_potentially_duplicated_assessments),
        )

        assessment_ids_set = set()
        for value in ordered_but_potentially_duplicated_assessments:
            assessment_ids_set.add(value[0])
        assessment_ids = tuple(assessment_ids_set)

        if len(assessment_ids) > 0:
            if len(assessment_ids) == 1:
                assessment_ids = f"({assessment_ids[0]})"
            select_assessments_sql = f"SELECT DISTINCT * from e_assessments WHERE id IN {assessment_ids};"
            assessments = self.connect_psql(select_assessments_sql)
        else:
            assessments = [()]
        print("assessments length: ", len(assessments))

        candidacies = self.get_candidacies(job_ids)
        print("candidacies length: ", len(candidacies))

        candidate_ids_set = set()
        if len(candidacies) > 0:
            for value in candidacies:
                # fields: ['id', 'user_id', 'job_id', 'pipeline_stage_id', 'added_to_stage_at', 'score', 'possible_score', 'weighted_percentage_score', 'archive_reason', 'parent_candidacy_id', 'status', 'progress', 'failed', 'withdraw_reason', 'percentile', 'completed_user_assessments', 'scoring_completed', 'external_job_id', 'remaining_assessment_count', 'created_at', 'updated_at', 'id', 'email', 'first_name', 'middle_name', 'last_name', 'country_code', 'phone', 'created_at', 'updated_at', 'id', 'name', 'sequence', 'slug', 'job_id', 'maintain_anonymity', 'type', 'created_at', 'updated_at']
                candidate_ids_set.add(value[0])

        candidate_ids = tuple(candidate_ids_set)

        non_pipeline_scoring_dimension_ids = []

        if len(candidate_ids) > 0:
            sql = """
                SELECT DISTINCT a.* FROM e_user_assessments as ua
                INNER JOIN e_candidacies as c ON ua.user_id=c.user_id
                INNER JOIN e_assessments as a ON a.id=ua.assessment_id
                WHERE c.job_id IN {} AND c.id IN {} AND a.id NOT IN {};
            """.format(
                job_ids, candidate_ids, assessment_ids
            )
            non_pipeline_assessments = self.connect_psql(sql)
        else:
            non_pipeline_assessments = [()]
        print("non_pipeline_assessments length: ", len(non_pipeline_assessments))

        for assessment in assessments:
            # fields: ['id', 'name', 'type', 'slug', 'created_at', 'updated_at']
            if len(assessment) <= 0:
                continue
            select_steps_sql = f"SELECT id FROM e_steps WHERE assessment_id={assessment[0]};"
            steps = self.connect_psql(select_steps_sql)
            for step in steps:
                select_scoring_rules_sql = f"SELECT id FROM e_scoring_rules WHERE step_id={step[0]}"
                scoring_rules = self.connect_psql(select_scoring_rules_sql)
                for sr in scoring_rules:
                    scoring_dimension_ids.append(sr[0])

        scoring_dimension_ids = tuple(set(scoring_dimension_ids))
        print("scoring_dimension_ids length:", len(scoring_dimension_ids))

        for assessment in non_pipeline_assessments:
            # fields: ['id', 'name', 'type', 'slug', 'created_at', 'updated_at']
            if len(assessment) <= 0:
                continue
            select_steps_sql = f"SELECT id FROM e_steps WHERE assessment_id={assessment[0]};"
            steps = self.connect_psql(select_steps_sql)
            for step in steps:
                select_scoring_rules_sql = f"SELECT id FROM e_scoring_rules WHERE step_id={step[0]}"
                scoring_rules = self.connect_psql(select_scoring_rules_sql)
                for sr in scoring_rules:
                    non_pipeline_scoring_dimension_ids.append(sr[0])

        non_pipeline_scoring_dimension_ids = tuple(set(non_pipeline_scoring_dimension_ids))
        print("non_pipeline_scoring_dimension_ids length:", len(non_pipeline_scoring_dimension_ids))

        if len(scoring_dimension_ids) > 0:
            if len(scoring_dimension_ids) == 1:
                scoring_dimension_ids = f"({scoring_dimension_ids[0]})"
            select_scoring_dimensions_sql = (
                f"SELECT DISTINCT * FROM e_scoring_dimensions WHERE id IN {scoring_dimension_ids};"
            )
            scoring_dimensions = self.connect_psql(select_scoring_dimensions_sql)
        else:
            scoring_dimensions = [()]
        print("scoring_dimensions length: ", len(scoring_dimensions))

        if len(non_pipeline_scoring_dimension_ids) > 0:
            if len(non_pipeline_scoring_dimension_ids) == 1:
                non_pipeline_scoring_dimension_ids = f"({non_pipeline_scoring_dimension_ids[0]})"
            select_non_pipeline_scoring_dimensions_sql = (
                f"SELECT DISTINCT * FROM e_scoring_dimensions WHERE id IN {non_pipeline_scoring_dimension_ids};"
            )
            non_pipeline_scoring_dimensions = self.connect_psql(select_non_pipeline_scoring_dimensions_sql)
        else:
            non_pipeline_scoring_dimensions = [()]
        print("non_pipeline_scoring_dimensions length: ", len(non_pipeline_scoring_dimensions))

        job_ids_str = "_".join(job_ids_str.replace(" ", "").split(","))
        path = cur_path + "/test_job_id_{}.csv".format(job_ids_str)
        with open(path, "a+") as file:
            csv_write = csv.writer(file)
            csv_headers = [
                "user_id",
                "user_last_name",
                "user_first_name",
                "user_email",
                "user_country_code",
                "user_phone",
                "candidacy_id",
                "candidacy_created_at",
                "candidacy_pipeline_stage",
                "candidacy_status",
                "candidacy_failed",
                "job_name",
                "tags",
                "percentile",
                "weighted_percentage_score",
                "assessments_remaining",
                "assessments_completed",
                "hours_since_application",
                "email_messages_count",
                "sms_messages_count",
                "last_email_created_at",
                "last_sms_created_at",
                "calendar_events",
            ]

            for assessment in assessments:
                # fields: ['id', 'name', 'type', 'slug', 'created_at', 'updated_at']
                if len(assessment) <= 0:
                    continue
                name = assessment[1].strip().lower().replace(" ", "_")
                assessment_headers = [
                    f"{name}_percentage",
                    f"{name}_percentile",
                    f"{name}_score",
                    f"{name}_started_at",
                    f"{name}_completed_at",
                ]
                csv_headers.extend(assessment_headers)

            for scoring_dimension in scoring_dimensions:
                # fields: ['id', 'name', 'organization_id']
                if len(scoring_dimension) <= 0:
                    continue
                name = scoring_dimension[1].strip().lower().replace(" ", "_")
                scoring_dimension_headers = [f"{name}_percentage", f"{name}_percentile"]
                csv_headers.extend(scoring_dimension_headers)

            for assessment in non_pipeline_assessments:
                # fields: ['id', 'name', 'type', 'slug', 'created_at', 'updated_at']
                if len(assessment) <= 0:
                    continue
                name = assessment[1].strip().lower().replace(" ", "_") + "_non_pipline"
                scoring_dimension_headers = [
                    f"{name}_percentage_NON_PIPELINE",
                    f"{name}_percentile_NON_PIPELINE",
                    f"{name}_score_NON_PIPELINE",
                    f"{name}_started_at_NON_PIPELINE",
                    f"{name}_completed_at_NON_PIPELINE",
                ]
                csv_headers.extend(scoring_dimension_headers)

            for sd in non_pipeline_scoring_dimensions:
                # fields: ['id', 'name', 'organization_id']
                if len(sd) <= 0:
                    continue
                name = sd[1].strip().lower().replace(" ", "_")
                sd_headers = [f"{name}_percentage_NON_PIPELINE", f"{name}_percentile_NON_PIPELINE"]
                csv_headers.extend(sd_headers)

            for custom_field in custom_fields:
                # fields: ['id', 'organization_id', 'name', 'slug', 'type']
                if len(custom_field) <= 0:
                    continue
                name = custom_field[2].strip()
                csv_headers.append(name)

            print("headers length: ", len(csv_headers))
            csv_write.writerow(csv_headers)

            for candidacy in candidacies:
                # field_names: [
                #     "e_candidacies_id",
                #     "e_candidacies_user_id",
                #     "e_candidacies_job_id",
                #     "e_candidacies_pipeline_stage_id",
                #     "e_candidacies_added_to_stage_at",
                #     "e_candidacies_score",
                #     "e_candidacies_possible_score",
                #     "e_candidacies_weighted_percentage_score",
                #     "e_candidacies_archive_reason",
                #     "e_candidacies_parent_candidacy_id",
                #     "e_candidacies_status",
                #     "e_candidacies_progress",
                #     "e_candidacies_failed",
                #     "e_candidacies_withdraw_reason",
                #     "e_candidacies_percentile",
                #     "e_candidacies_completed_user_assessments",
                #     "e_candidacies_scoring_completed",
                #     "e_candidacies_external_job_id",
                #     "e_candidacies_remaining_assessment_count",
                #     "e_candidacies_created_at",
                #     "e_candidacies_updated_at",
                #     "e_pipeline_stages_id",
                #     "e_pipeline_stages_name",
                #     "e_pipeline_stages_sequence",
                #     "e_pipeline_stages_slug",
                #     "e_pipeline_stages_job_id",
                #     "e_pipeline_stages_maintain_anonymity",
                #     "e_pipeline_stages_type",
                #     "e_pipeline_stages_created_at",
                #     "e_pipeline_stages_updated_at",
                #     "e_users_id",
                #     "e_users_email",
                #     "e_users_first_name",
                #     "e_users_middle_name",
                #     "e_users_last_name",
                #     "e_users_country_code",
                #     "e_users_phone",
                #     "e_users_created_at",
                #     "e_users_updated_at",
                # ]

                # print(candidacy)
                csv_values = []
                candidacy_dict = {}
                for field in CANDIDACY_FIELDS:
                    field_name = field.replace(".", "_")
                    field_index = CANDIDACY_FIELDS.index(field)
                    field_value = candidacy[field_index]
                    candidacy_dict.update({field_name: field_value})

                csv_values.extend(
                    [
                        candidacy_dict["e_users_id"],
                        candidacy_dict["e_users_last_name"],
                        candidacy_dict["e_users_first_name"],
                        candidacy_dict["e_users_email"],
                        candidacy_dict["e_users_country_code"],
                        candidacy_dict["e_users_phone"],
                        candidacy_dict["e_candidacies_id"],
                        candidacy_dict["e_candidacies_created_at"],
                        candidacy_dict["e_pipeline_stages_name"],
                        candidacy_dict["e_candidacies_status"],
                        candidacy_dict["e_candidacies_failed"],
                    ]
                )

                select_jobs_sql = """
                    SELECT name FROM e_jobs WHERE id={};
                """.format(
                    candidacy_dict["e_candidacies_job_id"]
                )
                job_name = self.connect_psql(select_jobs_sql)
                # [('TICA | Customer Service Representative El Salvador',)]
                csv_values.append(job_name[0][0])

                select_tags_name_sql = """
                    SELECT t.name FROM e_tags AS t
                    INNER JOIN e_candidacy_tags AS ct ON ct.tag_id=t.id WHERE ct.candidacy_id={}
                """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                tags_name = self.connect_psql(select_tags_name_sql)
                tags_name_str = "".join(tags_name[0]) if len(tags_name) > 0 else ""
                csv_values.append(tags_name_str)

                csv_values.extend(
                    [
                        candidacy_dict["e_candidacies_percentile"],
                        candidacy_dict["e_candidacies_weighted_percentage_score"],
                        candidacy_dict["e_candidacies_remaining_assessment_count"],
                    ]
                )

                select_user_assessments_completed_count_query = """
                                    SELECT count(ua.completed_at) 
                                    FROM e_candidacies AS c 
                                    INNER JOIN e_user_assessments AS ua ON c.user_id=ua.user_id 
                                    WHERE c.id={} AND ua.completed_at IS NOT NULL;
                                """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                user_assessments_completed_count = self.connect_psql(select_user_assessments_completed_count_query)
                csv_values.append(user_assessments_completed_count[0][0])

                csv_values.append(round((datetime.now() - candidacy_dict["e_candidacies_created_at"]).total_seconds()))

                select_events_email_query = """
                                        SELECT count(*) FROM e_events 
                                        WHERE candidacy_id={} AND type='email';
                                    """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                candidacies_events_email_count = self.connect_psql(select_events_email_query)

                select_events_sms_query = """
                                        SELECT count(*) FROM e_events 
                                        WHERE candidacy_id={} AND type='sms';
                                    """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                candidacies_events_sms_count = self.connect_psql(select_events_sms_query)

                select_events_email_last_query = """
                                        SELECT created_at FROM e_events 
                                        WHERE candidacy_id={} AND type='email' 
                                        ORDER BY id LIMIT 1;
                                    """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                candidacies_events_email_last_created_at = self.connect_psql(select_events_email_last_query)

                select_events_sms_last_query = """
                                        SELECT created_at FROM e_events 
                                        WHERE candidacy_id={} AND type='sms' 
                                        ORDER BY id LIMIT 1;
                                    """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                candidacies_events_sms_last_created_at = self.connect_psql(select_events_sms_last_query)

                select_calendar_events_query = """
                                    SELECT start_datetime FROM e_calendar_events WHERE candidacy_id={};
                                """.format(
                    candidacy_dict["e_candidacies_id"]
                )
                select_calendar_events_start_datetime = self.connect_psql(select_calendar_events_query)

                csv_values.extend(
                    [
                        candidacies_events_email_count[0][0] if len(
                            candidacies_events_email_count) > 0 else "",
                        candidacies_events_sms_count[0][0] if len(
                            candidacies_events_sms_count) > 0 else "",
                        candidacies_events_email_last_created_at[0][0] if len(
                            candidacies_events_email_last_created_at) > 0 else "",
                        candidacies_events_sms_last_created_at[0][0] if len(
                            candidacies_events_sms_last_created_at) > 0 else "",
                        select_calendar_events_start_datetime[0][0] if len(
                            select_calendar_events_start_datetime) > 0 else "",
                    ]
                )

                for assessment in assessments:
                    # fields: ['id', 'name', 'type', 'slug', 'created_at', 'updated_at']
                    select_candidacy_user_assessments_query = """
                                            SELECT ua.percentage_score, c.score, ua.started_at, ua.completed_at 
                                            FROM e_candidacies AS c 
                                            INNER JOIN e_user_assessments AS ua ON c.user_id=ua.user_id 
                                            WHERE c.id={} AND ua.assessment_id={};

                                        """.format(
                        candidacy_dict["e_candidacies_id"], assessment[0]
                    )
                    candidacy_user_assessments = self.connect_psql(select_candidacy_user_assessments_query)

                    if len(candidacy_user_assessments) > 0:
                        csv_values.extend(
                            [
                                candidacy_user_assessments[0][0],
                                candidacy_dict["e_candidacies_percentile"],
                                candidacy_user_assessments[0][1],
                                candidacy_user_assessments[0][2],
                                candidacy_user_assessments[0][3],
                            ]
                        )
                    else:
                        csv_values.extend(["", "", "", "", ""])

                for sd in scoring_dimensions:
                    select_sdr_query = """
                                   SELECT percentage_score 
                                   FROM e_scoring_dimension_ratings 
                                   WHERE candidacy_id={} AND scoring_dimension_id={}
                               """.format(
                        candidacy_dict["e_candidacies_id"], sd[0]
                    )
                    sdr = self.connect_psql(select_sdr_query)
                    csv_values.extend([sdr[0][0], candidacy_dict["e_candidacies_percentile"]])

                for assessment in non_pipeline_assessments:
                    # fields: ['id', 'name', 'type', 'slug', 'created_at', 'updated_at']
                    select_candidacy_user_assessments_query = """
                                            SELECT ua.percentage_score, c.score, ua.started_at, ua.completed_at 
                                            FROM e_candidacies AS c 
                                            INNER JOIN e_user_assessments AS ua ON c.user_id=ua.user_id 
                                            WHERE c.id={} AND ua.assessment_id={};

                                        """.format(
                        candidacy_dict["e_candidacies_id"], assessment[0]
                    )
                    candidacy_user_assessments = self.connect_psql(select_candidacy_user_assessments_query)

                    if len(candidacy_user_assessments) > 0:
                        csv_values.extend(
                            [
                                candidacy_user_assessments[0][0],
                                candidacy_dict["e_candidacies_percentile"],
                                candidacy_user_assessments[0][1],
                                candidacy_user_assessments[0][2],
                                candidacy_user_assessments[0][3],
                            ]
                        )
                    else:
                        csv_values.extend(["", "", "", "", ""])

                for sd in non_pipeline_scoring_dimensions:
                    select_sdr_query = """
                                SELECT percentage_score 
                                FROM e_scoring_dimension_ratings 
                                WHERE candidacy_id={} AND scoring_dimension_id={};
                            """.format(
                        candidacy_dict["e_candidacies_id"], sd[0]
                    )
                    sdr = self.connect_psql(select_sdr_query)
                    csv_values.extend([sdr[0][0], candidacy_dict["e_candidacies_percentile"]])

                for custom_field in custom_fields:
                    select_custom_field_query = """
                        SELECT value 
                        FROM e_answers 
                        WHERE question_id IN 
                        (SELECT id FROM e_questions WHERE custom_field_id={}) AND value<>'';
                    """.format(
                        custom_field[0]
                    )
                    custom_field_value = self.connect_psql(select_custom_field_query)
                    val = custom_field_value[0][0] if len(custom_field_value) > 0 else ""
                    csv_values.append(val)

                print("values length: ", len(csv_values))
                print("csv_values: ", csv_values)
                print("=" * 40)
                csv_write.writerow(csv_values)

    def run(self):
        # job_ids = (5475, 18764, 21575, 81298)
        cur_path = os.path.abspath(os.path.dirname(__file__))
        job_ids_str = input("Enter job IDs and separate with comma, example: 1,2,3\n")
        self.process(job_ids_str, cur_path)


if __name__ == "__main__":
    JobCandidates().run()
