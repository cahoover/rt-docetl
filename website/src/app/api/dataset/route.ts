import { NextRequest, NextResponse } from "next/server";
import { getBackendUrl } from "@/lib/api-config";

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const params = new URLSearchParams();
  const datasetUrl = searchParams.get("dataset_url");
  const sourceVersionId = searchParams.get("source_version_id");
  const projectId = searchParams.get("project_id");
  const subset = searchParams.get("subset");
  const env = searchParams.get("env");
  const namespace = searchParams.get("namespace") || "default";

  if (datasetUrl) params.set("dataset_url", datasetUrl);
  if (sourceVersionId) params.set("source_version_id", sourceVersionId);
  if (projectId) params.set("project_id", projectId);
  if (subset) params.set("subset", subset);
  if (env) params.set("env", env);
  params.set("namespace", namespace);

  const backendUrl = `${getBackendUrl()}/fs/dataset?${params.toString()}`;

  try {
    const response = await fetch(backendUrl);
    const payload = await response.text();
    if (!response.ok) {
      throw new Error(payload || "Failed to load dataset");
    }
    return NextResponse.json(JSON.parse(payload));
  } catch (error) {
    console.error("Dataset proxy error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to fetch dataset from backend",
      },
      { status: 500 }
    );
  }
}
