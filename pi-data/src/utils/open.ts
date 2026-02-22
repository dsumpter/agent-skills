import { spawn } from "child_process";
import { platform } from "os";

export function openFile(targetPath: string): void {
  const os = platform();
  if (os === "darwin") {
    const child = spawn("open", [targetPath], { stdio: "ignore", detached: true });
    child.unref();
    return;
  }
  if (os === "win32") {
    const child = spawn("cmd", ["/c", "start", "", targetPath], { stdio: "ignore", detached: true });
    child.unref();
    return;
  }
  const child = spawn("xdg-open", [targetPath], { stdio: "ignore", detached: true });
  child.unref();
}
