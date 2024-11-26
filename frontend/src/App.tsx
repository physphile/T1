import { FormEventHandler, useCallback, useState } from "react";
import axios from "axios";

export const App: React.FC = () => {
  const [headers, setHeaders] = useState<string[]>([]);
  // const [groups, setGroups] = useState({});

  const generateDatabase = useCallback<FormEventHandler>(async (event) => {
    event.preventDefault();
    const formData = new FormData(event.target as HTMLFormElement);
    await axios.post("http://localhost:8000/generate", formData);

    const {
      data: { headers },
    } = await axios.get<{ headers: string[] }>("http://localhost:8000/headers");

    setHeaders(headers);

    await axios.get("http://localhost:8000/groups");
  }, []);

  return (
    <div>
      <form onSubmit={generateDatabase}>
        <input type="file" name="files" multiple accept=".csv" />
        <button type="submit">Отправить</button>
      </form>

      <pre>{JSON.stringify(headers, null, 2)}</pre>
    </div>
  );
};

export default App;
