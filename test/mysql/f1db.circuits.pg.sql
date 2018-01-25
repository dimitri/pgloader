--
-- PostgreSQL database dump
--

-- Dumped from database version 10.1
-- Dumped by pg_dump version 10.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = f1db, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

CREATE SCHEMA f1db;

--
-- Name: circuits; Type: TABLE; Schema: f1db; Owner: dim
--

CREATE TABLE circuits (
    circuitid bigint NOT NULL,
    circuitref character varying(255) DEFAULT ''::character varying NOT NULL,
    name character varying(255) DEFAULT ''::character varying NOT NULL,
    location character varying(255) DEFAULT NULL::character varying,
    country character varying(255) DEFAULT NULL::character varying,
    position point,
    alt bigint,
    url character varying(255) DEFAULT ''::character varying NOT NULL
);


ALTER TABLE circuits OWNER TO dim;

--
-- Name: circuits_circuitid_seq; Type: SEQUENCE; Schema: f1db; Owner: dim
--

CREATE SEQUENCE circuits_circuitid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE circuits_circuitid_seq OWNER TO dim;

--
-- Name: circuits_circuitid_seq; Type: SEQUENCE OWNED BY; Schema: f1db; Owner: dim
--

ALTER SEQUENCE circuits_circuitid_seq OWNED BY circuits.circuitid;


--
-- Name: circuits circuitid; Type: DEFAULT; Schema: f1db; Owner: dim
--

ALTER TABLE ONLY circuits ALTER COLUMN circuitid SET DEFAULT nextval('circuits_circuitid_seq'::regclass);


--
-- Name: circuits idx_61484_primary; Type: CONSTRAINT; Schema: f1db; Owner: dim
--

ALTER TABLE ONLY circuits
    ADD CONSTRAINT idx_61484_primary PRIMARY KEY (circuitid);


--
-- Name: idx_61484_url; Type: INDEX; Schema: f1db; Owner: dim
--

CREATE UNIQUE INDEX idx_61484_url ON circuits USING btree (url);


--
-- PostgreSQL database dump complete
--

