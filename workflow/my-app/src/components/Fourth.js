import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Fourth() {
  const navigate = useNavigate();

  return (
    <div className="fourth-page">
      <div className="workflow-step-4">
        <h1 className="step-heading-4">Workflow – Step 3</h1>
        <p className="step-text-4">Context Recognition & Classification</p>

        <h3 className="step-heading-4">Objective:</h3>
        <p className="step-text-4">
          Detect whether a company is <b>Using</b>, <b>Hiring for</b>, or{" "}
          <b>Discussing</b> a specific technology.
        </p>

        <h3 className="step-heading-4">Model Workstreams:</h3>
        <table className="workflow-table">
          <thead>
            <tr>
              <th>Model Type</th>
              <th>Purpose</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Cosine Similarity</td>
              <td>Baseline context similarity model</td>
              <td>Refinement Stage</td>
            </tr>
            <tr>
              <td>Gemini API (Google)</td>
              <td>LLM-based prompt response & context logic</td>
              <td>Refinement</td>
            </tr>
            <tr>
              <td>Amazon Bedrock</td>
              <td>Unified model layer (Claude, Titan, etc.)</td>
              <td>Training</td>
            </tr>
            <tr>
              <td>Claude-based Model</td>
              <td>Rule-based structured judgment</td>
              <td>Fine-tuning</td>
            </tr>
            <tr>
              <td>Other Model Research</td>
              <td>Discovering better or faster models</td>
              <td>Ongoing</td>
            </tr>
          </tbody>
        </table>

        <h3 className="step-heading-4">Output Format Example:</h3>
        <pre className="output-box">
{`{
  "verdict": "Using",
  "reasoning": "...",
  "confidence": "High"
}`}
        </pre>

        {/* Arrow Button */}
        <button
          className="arrow-btn"
          onClick={() => navigate("/summary")}
          aria-label="Go to Next Step"
        >
          ➔
        </button>
      </div>
    </div>
  );
}
