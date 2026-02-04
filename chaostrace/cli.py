"""
ChaosTrace CLI

Command-line interface for managing test runs with CI/CD support.
"""

import json
import sys
from pathlib import Path

import httpx
import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = typer.Typer(
    name="chaostrace",
    help="AI Agent Chaos Testing Platform",
    add_completion=False,
)

console = Console()

# Default API URL
DEFAULT_API_URL = "http://localhost:8000/api"


def get_api_url() -> str:
    """Get API URL from environment or default."""
    import os
    return os.getenv("CHAOSTRACE_API_URL", DEFAULT_API_URL)


@app.command()
def run(
    agent: str = typer.Option(..., "--agent", "-a", help="Path to agent script"),
    scenario: str = typer.Option(..., "--scenario", "-s", help="Scenario name"),
    policy: str = typer.Option("strict", "--policy", "-p", help="Policy profile"),
    chaos: str = typer.Option(None, "--chaos", "-c", help="Chaos profile"),
    timeout: int = typer.Option(300, "--timeout", "-t", help="Timeout in seconds"),
    output: str = typer.Option(None, "--output", "-o", help="Output file for report"),
    format: str = typer.Option("json", "--format", "-f", help="Output format (json, markdown)"),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for completion"),
    threshold: int = typer.Option(70, "--threshold", help="Pass threshold score"),
):
    """
    Create and run a new chaos test.
    
    Examples:
        chaostrace run -a examples/cleanup_agent.py -s data_cleanup
        chaostrace run -a my_agent.py -s rogue_admin -p strict -c db_lock_v1
    """
    api_url = get_api_url()
    
    # Create run request
    request_data = {
        "agent_type": "python",
        "agent_entry": agent,
        "scenario": scenario,
        "policy_profile": policy,
        "chaos_profile": chaos,
        "timeout_seconds": timeout,
    }
    
    console.print(Panel.fit(
        f"[bold blue]ChaosTrace[/bold blue] - AI Agent Safety Test\n\n"
        f"Agent: [cyan]{agent}[/cyan]\n"
        f"Scenario: [yellow]{scenario}[/yellow]\n"
        f"Policy: [green]{policy}[/green]\n"
        f"Timeout: {timeout}s",
        title="🌀 Starting Test"
    ))
    
    try:
        with httpx.Client(timeout=30) as client:
            # Create the run
            response = client.post(f"{api_url}/runs", json=request_data)
            response.raise_for_status()
            run_data = response.json()
            run_id = run_data["run_id"]
            
            console.print(f"\n[dim]Run ID: {run_id}[/dim]")
            
            if not wait:
                console.print("[yellow]Run started in background. Use 'chaostrace status' to check.[/yellow]")
                return
            
            # Wait for completion
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Running test...", total=None)
                
                while True:
                    import time
                    time.sleep(2)
                    
                    status_response = client.get(f"{api_url}/runs/{run_id}")
                    status_response.raise_for_status()
                    status = status_response.json()
                    
                    progress.update(task, description=f"Status: {status['status']}")
                    
                    if status["status"] in ["completed", "failed", "error", "timeout"]:
                        break
            
            # Get the report
            report_response = client.get(
                f"{api_url}/reports/{run_id}",
                params={"format": format}
            )
            
            if report_response.status_code == 200:
                if format == "markdown":
                    report = report_response.text
                else:
                    report = report_response.json()
                
                # Save to file if requested
                if output:
                    output_path = Path(output)
                    if format == "markdown":
                        output_path.write_text(report)
                    else:
                        output_path.write_text(json.dumps(report, indent=2))
                    console.print(f"\n[dim]Report saved to: {output}[/dim]")
                
                # Display results
                if format == "json":
                    score = report.get("score", {}).get("final_score", 0)
                    grade = report.get("score", {}).get("grade", "?")
                    passed = report.get("ci", {}).get("pass", False)
                    
                    # Color based on grade
                    grade_color = {
                        "A": "green", "B": "green", "C": "yellow", "D": "red", "F": "red"
                    }.get(grade, "white")
                    
                    console.print(f"\n[bold]Results:[/bold]")
                    console.print(f"  Score: [{grade_color}]{score}/100[/{grade_color}]")
                    console.print(f"  Grade: [{grade_color}]{grade}[/{grade_color}]")
                    console.print(f"  Pass:  {'[green]✓ Yes[/green]' if passed else '[red]✗ No[/red]'}")
                    
                    # Exit with appropriate code for CI
                    if not passed or score < threshold:
                        console.print(f"\n[red]❌ Test failed (score {score} < threshold {threshold})[/red]")
                        raise typer.Exit(1)
                    else:
                        console.print(f"\n[green]✅ Test passed![/green]")
                else:
                    console.print(report)
            else:
                console.print("[yellow]Could not generate report[/yellow]")
                
    except httpx.HTTPError as e:
        console.print(f"[red]API Error: {e}[/red]")
        raise typer.Exit(2)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(2)


@app.command("list")
def list_runs(
    limit: int = typer.Option(20, "--limit", "-l", help="Number of runs to show"),
):
    """List recent test runs."""
    api_url = get_api_url()
    
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(f"{api_url}/runs", params={"page_size": limit})
            response.raise_for_status()
            data = response.json()
        
        runs = data.get("runs", [])
        
        table = Table(title="Recent Runs")
        table.add_column("Run ID", style="cyan")
        table.add_column("Scenario")
        table.add_column("Status")
        table.add_column("Verdict")
        table.add_column("Score")
        table.add_column("Created")
        
        for run in runs:
            verdict = run.get("verdict", "-")
            verdict_style = {
                "pass": "green", "fail": "red", "warn": "yellow"
            }.get(verdict, "dim")
            
            table.add_row(
                run["run_id"][:8] + "...",
                run.get("scenario", "-"),
                run.get("status", "-"),
                f"[{verdict_style}]{verdict}[/{verdict_style}]",
                str(run.get("score", "-")),
                run.get("created_at", "-")[:19],
            )
        
        console.print(table)
        console.print(f"\n[dim]Total: {data.get('total', len(runs))} runs[/dim]")
        
    except httpx.HTTPError as e:
        console.print(f"[red]API Error: {e}[/red]")
        console.print("[dim]Make sure the ChaosTrace server is running[/dim]")


@app.command()
def report(
    run_id: str = typer.Argument(..., help="Run ID to get report for"),
    format: str = typer.Option("json", "--format", "-f", help="Output format (json, markdown)"),
    output: str = typer.Option(None, "--output", "-o", help="Output file"),
):
    """Get report for a specific run."""
    api_url = get_api_url()
    
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(
                f"{api_url}/reports/{run_id}",
                params={"format": format}
            )
            response.raise_for_status()
            
            if format == "markdown":
                report_content = response.text
            else:
                report_content = json.dumps(response.json(), indent=2)
            
            if output:
                Path(output).write_text(report_content)
                console.print(f"[green]Report saved to: {output}[/green]")
            else:
                console.print(report_content)
                
    except httpx.HTTPError as e:
        console.print(f"[red]API Error: {e}[/red]")


@app.command()
def status(run_id: str = typer.Argument(..., help="Run ID to check")):
    """Check status of a specific run."""
    api_url = get_api_url()
    
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(f"{api_url}/runs/{run_id}")
            response.raise_for_status()
            data = response.json()
        
        console.print(Panel.fit(
            f"Run ID: [cyan]{data['run_id']}[/cyan]\n"
            f"Status: {data['status']}\n"
            f"Verdict: {data.get('verdict', '-')}\n"
            f"SQL Events: {data.get('total_sql_events', 0)}\n"
            f"Blocked: {data.get('blocked_events', 0)}\n"
            f"Chaos Events: {data.get('chaos_events_triggered', 0)}",
            title="Run Status"
        ))
        
    except httpx.HTTPError as e:
        console.print(f"[red]API Error: {e}[/red]")


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host to bind"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to bind"),
    reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
):
    """Start the ChaosTrace API server."""
    import uvicorn
    
    console.print(Panel.fit(
        f"[bold green]ChaosTrace Server[/bold green]\n\n"
        f"API: http://{host}:{port}/api\n"
        f"Dashboard: http://{host}:{port}/\n"
        f"Docs: http://{host}:{port}/docs",
        title="🌀 Starting Server"
    ))
    
    uvicorn.run(
        "chaostrace.control_plane.main:app",
        host=host,
        port=port,
        reload=reload,
    )


@app.command()
def validate(
    policy: str = typer.Option(None, "--policy", "-p", help="Policy file to validate"),
    chaos: str = typer.Option(None, "--chaos", "-c", help="Chaos script to validate"),
    scenario: str = typer.Option(None, "--scenario", "-s", help="Scenario file to validate"),
):
    """Validate configuration files."""
    import yaml
    
    files_to_check = []
    if policy:
        files_to_check.append(("policy", policy))
    if chaos:
        files_to_check.append(("chaos", chaos))
    if scenario:
        files_to_check.append(("scenario", scenario))
    
    if not files_to_check:
        console.print("[yellow]No files specified. Use --policy, --chaos, or --scenario[/yellow]")
        return
    
    all_valid = True
    for file_type, file_path in files_to_check:
        try:
            with open(file_path) as f:
                data = yaml.safe_load(f)
            
            # Basic validation
            if "name" not in data:
                console.print(f"[yellow]⚠ {file_path}: Missing 'name' field[/yellow]")
            
            console.print(f"[green]✓ {file_path}: Valid {file_type} file[/green]")
            
        except FileNotFoundError:
            console.print(f"[red]✗ {file_path}: File not found[/red]")
            all_valid = False
        except yaml.YAMLError as e:
            console.print(f"[red]✗ {file_path}: Invalid YAML - {e}[/red]")
            all_valid = False
    
    if not all_valid:
        raise typer.Exit(1)


@app.command()
def version():
    """Show version information."""
    from chaostrace import __version__
    console.print(f"ChaosTrace v{__version__}")


if __name__ == "__main__":
    app()
