# AREMS - Agentic Risk & Environmental Management System

AREMS is a disaster management platform built on Google Cloud that bridges AI, geospatial analytics, and community engagement. It empowers humanitarian organizations and vulnerable communities to collect, analyze, and act on risk data through conversational interfaces like WhatsApp and Telegram.

## Features

- **Chatbot Interface**: Receive and process risk reports via WhatsApp/Telegram bots.
- **AI-Powered Analysis**: Automated classification and validation of incident reports using Vertex AI.
- **Community-Sourced Mapping**: Real-time community data visualized as risk maps.
- **Preparedness Training**: Adaptive content delivery and quiz modules for community leaders.
- **Admin Dashboard**: Oversee campaign progress, review submissions, and manage user training.

## Project Structure

- `infrastructure/terraform/` – Infrastructure as Code for GCP resources.
- `functions/` – Cloud Functions and Cloud Run services.
- `frontend/` – Web interface and admin dashboard code.
- `webhook/` – Dialogflow webhook logic containerized for Cloud Run.

## Getting Started

This repository is a work in progress and will expand as new features and modules are developed.

---

> *This project is inspired by and demonstrates the real-world application of cloud architecture and AI for humanitarian impact.
