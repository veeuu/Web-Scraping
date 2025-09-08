import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function First() {
  const navigate = useNavigate();

  return (
    <div className="App">
      <div className="workflow-container">
        <h1 className="workflow-title">
          AI & Automation Team – Current Workflow & Task Structure
        </h1>
        <p className="workflow-subtext">
          Building scalable and collaborative context recognition for technographic
          analysis.
        </p>
      </div>
      <button
        className="arrow-btn"
        onClick={() => navigate("/second")}
        aria-label="Go to Second Page"
      >
        ➔
      </button>
    </div>
  );
}
